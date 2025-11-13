/*
Copyright 2025 The OpenCIDN Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runner

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	taskv1alpha1 "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	"github.com/OpenCIDN/cidn/pkg/versions"
	"github.com/wzshiming/ioswmr"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

var (
	ErrBearerNotReady = errors.New("bearer is not ready")
	ErrAuthentication = errors.New("authentication error")
	ErrNoPendingChunk = fmt.Errorf("no pending chunks available")
)

// ChunkRunner executes Chunk tasks
type ChunkRunner struct {
	handlerName    string
	client         versioned.Interface
	chunkInformer  informers.ChunkInformer
	bearerInformer informers.BearerInformer
	httpClient     *http.Client
	signal         chan struct{}
	updateDuration time.Duration
	concurrency    int
}

// NewChunkRunner creates a new Runner instance
func NewChunkRunner(
	handlerName string,
	clientset versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
	updateDuration time.Duration,
	concurrency int,
) *ChunkRunner {
	chunkInformer := taskv1alpha1.New(sharedInformerFactory, "", func(opt *metav1.ListOptions) {
		opt.FieldSelector = "status.handlerName=,status.phase=Pending"
	}).Chunks()
	r := &ChunkRunner{
		handlerName:    handlerName,
		client:         clientset,
		chunkInformer:  chunkInformer,
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		httpClient:     http.DefaultClient,
		signal:         make(chan struct{}, 1),
		updateDuration: updateDuration,
		concurrency:    concurrency,
	}

	r.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			r.enqueueChunk()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			r.enqueueChunk()
		},
	})
	r.bearerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			r.enqueueChunk()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			r.enqueueChunk()
		},
	})

	return r
}

func (r *ChunkRunner) enqueueChunk() {
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

// Release releases the current held chunk
func (r *ChunkRunner) Release(ctx context.Context) error {
	chunks, err := r.chunkInformer.Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	var wg sync.WaitGroup

	for _, chunk := range chunks {
		if chunk.Status.HandlerName != r.handlerName {
			continue
		}

		if chunk.Status.Phase != v1alpha1.ChunkPhasePending && chunk.Status.Phase != v1alpha1.ChunkPhaseRunning {
			continue
		}

		klog.Infof("Releasing chunk %s (current phase: %s)", chunk.Name, chunk.Status.Phase)
		wg.Add(1)
		go func(s *v1alpha1.Chunk) {
			defer wg.Done()

			_, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Chunks(), s, func(chunk *v1alpha1.Chunk) *v1alpha1.Chunk {
				chunk.Status.HandlerName = ""
				chunk.Status.Phase = v1alpha1.ChunkPhasePending
				chunk.Status.Conditions = nil
				return chunk
			})
			if err != nil {
				klog.Errorf("failed to release chunk %s: %v", s.Name, err)
			}
		}(chunk)
	}

	return nil
}

// Shutdown stops the runner
func (r *ChunkRunner) Shutdown(ctx context.Context) error {
	return r.Release(ctx)
}

// Run starts the runner
func (r *ChunkRunner) Start(ctx context.Context) error {
	go r.runWorker(ctx)

	return nil
}

func (r *ChunkRunner) runWorker(ctx context.Context) {
	for r.processNextItem(ctx) {
	}
}

func (r *ChunkRunner) processNextItem(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	chunks, err := r.getPendingList()
	if err != nil {
		klog.Errorf("failed to list pending chunks: %v", err)
		select {
		case <-r.signal:
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return false
		}
		return true
	}

	wg := sync.WaitGroup{}

	err = r.handlePending(context.Background(), chunks, func(c *v1alpha1.Chunk) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			continues := make(chan struct{})
			go r.process(continues, c.DeepCopy())
			continues <- struct{}{}
			close(continues)
		}()

	})
	if err != nil {
		if errors.Is(err, ErrNoPendingChunk) {
			klog.Infof("no pending chunks available")
		} else {
			klog.Errorf("failed to get pending chunk: %v", err)
		}

		select {
		case <-r.signal:
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return false
		}
		return true
	}

	wg.Wait()

	return true
}

// buildRequest constructs an HTTP request from ChunkHTTP configuration
func (r *ChunkRunner) buildRequest(ctx context.Context, chunkHTTP *v1alpha1.ChunkHTTP, body io.Reader, contentLength int64) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, chunkHTTP.Request.Method, chunkHTTP.Request.URL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	// Set default headers
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", versions.DefaultUserAgent())
	if contentLength > 0 {
		req.ContentLength = contentLength
	}

	// Add custom headers from configuration
	for k, v := range chunkHTTP.Request.Headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

// tryAddBearer fetches the bearer token and adds the Authorization header to the chunk
func (r *ChunkRunner) tryAddBearer(ctx context.Context, chunk *v1alpha1.Chunk) error {
	if chunk.Spec.BearerName == "" {
		return nil
	}
	bearer, err := r.bearerInformer.Lister().Get(chunk.Spec.BearerName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if bearer == nil {
		return nil
	}
	if bearer.Status.Phase == v1alpha1.BearerPhaseFailed {
		return fmt.Errorf("%w: bearer %s is in failed phase", ErrAuthentication, bearer.Name)
	}
	if bearer.Status.TokenInfo == nil || bearer.Status.TokenInfo.Token == "" {
		if bearer.Status.Phase == v1alpha1.BearerPhaseSucceeded {
			return fmt.Errorf("bearer %s is in succeeded phase but has no token info", bearer.Name)
		}
		return fmt.Errorf("%w: bearer %s is not in succeeded phase (current: %s)", ErrBearerNotReady, bearer.Name, bearer.Status.Phase)
	}

	if chunk.Spec.Source.Request.Headers == nil {
		chunk.Spec.Source.Request.Headers = make(map[string]string)
	}
	chunk.Spec.Source.Request.Headers["Authorization"] = "Bearer " + bearer.Status.TokenInfo.Token

	issuedAt := bearer.Status.TokenInfo.IssuedAt.Time
	expiresIn := bearer.Status.TokenInfo.ExpiresIn

	if expiresIn > 0 && !issuedAt.IsZero() {
		since := time.Since(issuedAt)
		expires := time.Duration(expiresIn) * time.Second

		if since >= expires {
			_, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Bearers(), bearer, func(b *v1alpha1.Bearer) *v1alpha1.Bearer {
				b.Status.HandlerName = ""
				b.Status.Phase = v1alpha1.BearerPhasePending
				return b
			})
			if err != nil {
				return err
			}

			return fmt.Errorf("%w: bearer %s token has expired", ErrBearerNotReady, bearer.Name)
		}

		if since >= expires*3/4 {
			_, err := utils.UpdateResourceStatusWithRetry(context.Background(), r.client.TaskV1alpha1().Bearers(), bearer, func(b *v1alpha1.Bearer) *v1alpha1.Bearer {
				b.Status.HandlerName = ""
				b.Status.Phase = v1alpha1.BearerPhasePending
				return b
			})
			if err != nil {
				klog.Errorf("Failed to update bearer %s status: %v", bearer.Name, err)
			}
		}
	}

	return nil
}

func (r *ChunkRunner) sourceRequest(ctx context.Context, chunk *v1alpha1.Chunk, s *state) (io.ReadCloser, int64) {
	err := r.tryAddBearer(ctx, chunk)
	if err != nil {
		if errors.Is(err, ErrBearerNotReady) {
			// Release the chunk back to Pending state to wait for bearer to be ready
			s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
				klog.Infof("Releasing chunk %s because bearer is not ready", ss.Name)
				ss.Status.HandlerName = ""
				ss.Status.Phase = v1alpha1.ChunkPhasePending
				ss.Status.Conditions = nil
				return ss
			})
			return nil, 0
		} else if errors.Is(err, ErrAuthentication) {
			s.handleProcessError("AuthenticationError", err)
		} else {
			s.handleProcessErrorAndRetryable("WaitingForBearer", err)
		}
		return nil, 0
	}

	srcReq, err := r.buildRequest(ctx, &chunk.Spec.Source, nil, 0)
	if err != nil {
		retry, err := utils.IsNetworkError(err)
		if retry {
			s.handleProcessErrorAndRetryable("BuildRequestNetworkError", err)
		} else {
			s.handleProcessError("BuildRequestError", err)
		}
		return nil, 0
	}

	srcResp, err := r.httpClient.Do(srcReq)
	retry, err := utils.IsHTTPResponseError(srcResp, err)
	if err != nil {
		if srcResp != nil && srcResp.Body != nil {
			srcResp.Body.Close()
		}
		if retry {
			s.handleProcessErrorAndRetryable("SourceRequestNetworkError", err)
		} else {
			s.handleProcessError("SourceRequestError", err)
		}
		return nil, 0
	}

	headers := map[string]string{}
	for k := range srcResp.Header {
		headers[strings.ToLower(k)] = srcResp.Header.Get(k)
	}

	s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
		ss.Status.SourceResponse = &v1alpha1.ChunkHTTPResponse{
			StatusCode: srcResp.StatusCode,
			Headers:    headers,
		}
		return ss
	})

	if chunk.Spec.Source.Response.StatusCode != 0 {
		if srcResp.StatusCode != chunk.Spec.Source.Response.StatusCode {
			if srcResp.StatusCode == http.StatusUnauthorized &&
				srcReq.Header.Get("Authorization") == "" &&
				chunk.Spec.BearerName != "" {
				err := fmt.Errorf("unauthorized access to source URL")
				s.handleProcessErrorAndRetryable("Unauthorized", err)

			} else {
				err := fmt.Errorf("unexpected status code: got %d, want %d",
					srcResp.StatusCode, chunk.Spec.Source.Response.StatusCode)
				s.handleProcessError("UnexpectedStatusCode", err)
			}
			if srcResp.Body != nil {
				srcResp.Body.Close()
			}
			return nil, 0
		}
	} else {
		if srcResp.StatusCode >= http.StatusMultipleChoices {
			if srcResp.StatusCode == http.StatusUnauthorized &&
				srcReq.Header.Get("Authorization") == "" &&
				chunk.Spec.BearerName != "" {
				err := fmt.Errorf("unauthorized access to source URL")
				s.handleProcessErrorAndRetryable("Unauthorized", err)
			} else {
				err := fmt.Errorf("source returned error status code: %d", srcResp.StatusCode)
				s.handleProcessError("ErrorStatusCode", err)
			}

			if srcResp.Body != nil {
				srcResp.Body.Close()
			}
			return nil, 0
		}
	}

	if srcResp.ContentLength > 0 &&
		chunk.Spec.Total > 0 &&
		srcResp.ContentLength != chunk.Spec.Total {
		err := fmt.Errorf("content length mismatch: got %d, want %d", srcResp.ContentLength, chunk.Spec.Total)
		s.handleProcessError("ContentLengthMismatch", err)

		if srcResp.Body != nil {
			srcResp.Body.Close()
		}
		return nil, 0
	}

	for k, v := range chunk.Spec.Source.Response.Headers {
		respVal := srcResp.Header.Get(k)
		if respVal != v {
			err := fmt.Errorf("header %s mismatch: got %s, want %s", k, respVal, v)
			s.handleProcessError("HeaderMismatch", err)

			if srcResp.Body != nil {
				srcResp.Body.Close()
			}
			return nil, 0
		}
	}

	return srcResp.Body, srcResp.ContentLength
}

func (r *ChunkRunner) destinationRequest(ctx context.Context, dest *v1alpha1.ChunkHTTP, dr *swmrCount, contentLength int64) (string, error) {
	destReq, err := r.buildRequest(ctx, dest, dr.NewReader(), contentLength)
	if err != nil {
		if retry, err := utils.IsNetworkError(err); !retry {
			return "", fmt.Errorf("failed to build destination request: %w", err)
		}

		destReq, err = r.buildRequest(ctx, dest, dr.NewReader(), contentLength)
		if err != nil {
			return "", fmt.Errorf("retry: failed to build destination request: %w", err)
		}
	}

	destResp, err := r.httpClient.Do(destReq)
	if err != nil {
		if retry, err := utils.IsHTTPResponseError(destResp, err); !retry {
			return "", fmt.Errorf("failed to perform destination request: %w", err)
		}

		destReq, err = r.buildRequest(ctx, dest, dr.NewReader(), contentLength)
		if err != nil {
			return "", fmt.Errorf("retry prepare: failed to build destination request: %w", err)
		}
		destResp, err = r.httpClient.Do(destReq)
		if err != nil {
			return "", fmt.Errorf("retry: failed to perform destination request: %w", err)
		}
	}
	defer destResp.Body.Close()

	if dest.Response.StatusCode != 0 {
		if destResp.StatusCode != dest.Response.StatusCode {
			body, err := io.ReadAll(destResp.Body)
			if err != nil {
				return "", fmt.Errorf("unexpected status code from destination: got %d, want %d (failed to read response body: %v)",
					destResp.StatusCode, dest.Response.StatusCode, err)
			}
			return "", fmt.Errorf("unexpected status code from destination: got %d, want %d, body: %s",
				destResp.StatusCode, dest.Response.StatusCode, string(body))
		}
	} else {
		if destResp.StatusCode >= http.StatusMultipleChoices {
			body, err := io.ReadAll(destResp.Body)
			if err != nil {
				return "", fmt.Errorf("destination returned error status code: %d (failed to read response body: %v)", destResp.StatusCode, err)
			}
			return "", fmt.Errorf("destination returned error status code: %d, body: %s", destResp.StatusCode, string(body))
		}
	}

	etag := destResp.Header.Get("ETag")
	if uetag, err := strconv.Unquote(etag); err == nil && uetag != "" {
		etag = uetag
	}

	if etag == "" {
		return "", fmt.Errorf("empty ETag received from destination")
	}

	return etag, nil
}

func (r *ChunkRunner) process(continues <-chan struct{}, chunk *v1alpha1.Chunk) {
	klog.Infof("Processing chunk %s (handler: %s)", chunk.Name, chunk.Status.HandlerName)
	defer klog.Infof("Finish processing chunk %s", chunk.Name)
	defer func() {
		<-continues
	}()

	s := newState(chunk)

	var gsr *readCount
	var gdrs []*swmrCount

	stopProgress := r.startProgressUpdater(context.Background(), s, &gsr, &gdrs)
	defer stopProgress()

	g, ctx := errgroup.WithContext(context.Background())

	body, contentLength := r.sourceRequest(ctx, chunk, s)
	if body == nil {
		return
	}

	if contentLength > 0 {
		if chunk.Spec.Total > 0 && contentLength != chunk.Spec.Total {
			err := fmt.Errorf("content length mismatch: got %d, want %d", contentLength, chunk.Spec.Total)
			s.handleProcessError("ContentLengthMismatch", err)
			body.Close()
			return
		}
	} else {
		if chunk.Spec.Total > 0 {
			contentLength = chunk.Spec.Total
		}
	}

	if len(chunk.Spec.Destination) == 0 {
		if chunk.Spec.InlineResponseBody {
			body, err := io.ReadAll(body)
			if err != nil {
				s.handleProcessError("ReadSourceBodyError", err)
				return
			}
			s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
				ss.Status.ResponseBody = body
				utils.SetChunkTerminalPhase(ss, v1alpha1.ChunkPhaseSucceeded)
				return ss
			})
		} else {
			s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
				utils.SetChunkTerminalPhase(ss, v1alpha1.ChunkPhaseSucceeded)
				return ss
			})
		}
		return
	}

	f, err := os.CreateTemp("", "cidn-chunk-")
	if err == nil {
		defer func() {
			f.Close()
			os.Remove(f.Name())
		}()
	}

	swmr := ioswmr.NewSWMR(f)

	sr := newReadCount(ctx, body)
	g.Go(func() error {
		_, err := io.Copy(swmr, sr)
		if err != nil {
			return err
		}
		return swmr.Close()
	})

	etags := make([]string, len(chunk.Spec.Destination))
	drs := make([]*swmrCount, 0, len(chunk.Spec.Destination))

	for _, dest := range chunk.Spec.Destination {
		dest := dest
		if dest.Request.Method == "" {
			continue
		}
		dr := newSWMRCount(ctx, swmr)
		drs = append(drs, dr)
	}

	s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
		gsr = sr
		gdrs = drs
		return ss
	})

	if contentLength <= 0 {
		err = g.Wait()
		if err != nil {
			s.handleProcessError("ReadSourceError", err)
			return
		}

		contentLength = sr.Count()
	}

	for i, dest := range chunk.Spec.Destination {
		dest := dest
		if dest.Request.Method == "" {
			continue
		}
		i := i
		dr := drs[i]
		g.Go(func() error {
			etag, err := r.destinationRequest(ctx, &dest, dr, contentLength)
			if err != nil {
				return err
			}
			etags[i] = etag
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		s.handleProcessError("DestinationRequestError", err)
		return
	}

	r.handleSha256AndFinalize(continues, chunk, s, swmr, etags)
}

func (r *ChunkRunner) startProgressUpdater(ctx context.Context, s *state, gsr **readCount, gdrs *[]*swmrCount) func() {
	chunkFunc := func() {
		s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
			if *gsr != nil {
				updateProgress(&ss.Status, &ss.Spec, *gsr, *gdrs)
			}

			s := ss.Status.DeepCopy()
			chunk, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Chunks(), ss, func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
				ss.Status = *s
				return ss
			})

			if err != nil {
				handleProcessError(ss, "ProgressUpdateError", err)
				return ss
			}

			return chunk
		})
	}

	dur := r.updateDuration
	ticker := time.NewTicker(dur)
	stop := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				chunkFunc()
				dur = r.updateDuration + time.Duration(rand.Intn(100))*time.Millisecond
				ticker.Reset(dur)
			case <-stop:
				chunkFunc()
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() { close(stop) }
}

func (r *ChunkRunner) handleSha256AndFinalize(continues <-chan struct{}, chunk *v1alpha1.Chunk, s *state, swmr ioswmr.SWMR, etags []string) {
	if chunk.Spec.Sha256PartialPreviousName == "" {
		s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
			ss.Status.Etags = etags
			utils.SetChunkTerminalPhase(ss, v1alpha1.ChunkPhaseSucceeded)
			return ss
		})
		return
	}
	if chunk.Spec.Sha256PartialPreviousName == "-" {
		sha256, sha256Partial, err := updateSha256(chunk.Spec.Sha256, nil, swmr.NewReader())
		if err != nil {
			s.handleProcessError("Sha256UpdateError", err)
			return
		}

		s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
			ss.Status.Sha256 = sha256
			ss.Status.Sha256Partial = sha256Partial
			ss.Status.Etags = etags
			utils.SetChunkTerminalPhase(ss, v1alpha1.ChunkPhaseSucceeded)
			return ss
		})
		return
	}

	<-continues
	r.waitForPartialChunk(chunk, s, swmr, etags)
}

func (r *ChunkRunner) waitForPartialChunk(chunk *v1alpha1.Chunk, s *state, swmr ioswmr.SWMR, etags []string) {
	chunks := r.client.TaskV1alpha1().Chunks()
	for {
		pchunk, err := chunks.Get(context.Background(), chunk.Spec.Sha256PartialPreviousName, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				s.handleProcessError("GetPartialChunkError", err)
				return
			}
			time.Sleep(time.Second)
			continue
		}

		if pchunk.Status.Phase != v1alpha1.ChunkPhaseSucceeded {
			klog.Infof("Waiting for partial chunk %s to succeed for chunk %s", pchunk.Name, chunk.Name)
			time.Sleep(time.Second)
			continue
		}
		if len(pchunk.Status.Sha256Partial) == 0 {
			err := fmt.Errorf("partial chunk %q has no sha256 partial data", chunk.Spec.Sha256PartialPreviousName)
			s.handleProcessError("MissingSha256PartialData", err)
			return
		}

		sha256, sha256Partial, err := updateSha256(chunk.Spec.Sha256, pchunk.Status.Sha256Partial, swmr.NewReader())
		if err != nil {
			s.handleProcessError("Sha256UpdateError", err)
			return
		}

		s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
			ss.Status.Sha256 = sha256
			ss.Status.Sha256Partial = sha256Partial
			ss.Status.Etags = etags
			utils.SetChunkTerminalPhase(ss, v1alpha1.ChunkPhaseSucceeded)
			return ss
		})

		klog.Infof("Chunk %s succeeded after waiting for partial chunk %s", chunk.Name, pchunk.Name)
		return
	}
}

func updateProgress(status *v1alpha1.ChunkStatus, spec *v1alpha1.ChunkSpec, sr *readCount, drs []*swmrCount) {
	var progress int64
	sourceProgress := sr.Count()

	progress += sourceProgress

	destinationProgresses := make([]int64, 0, len(spec.Destination))
	for _, dr := range drs {
		destinationProgress := dr.Count()
		progress += destinationProgress
		destinationProgresses = append(destinationProgresses, destinationProgress)
	}

	status.Progress = progress / int64(len(spec.Destination)+1)
	status.SourceProgress = sourceProgress
	status.DestinationProgresses = destinationProgresses
}

func updateSha256(sha256 string, sha256Partial []byte, reader io.Reader) (string, []byte, error) {
	hash := newSha256()

	if len(sha256Partial) > 0 {
		err := hash.UnmarshalBinary(sha256Partial)
		if err != nil {
			return "", nil, err
		}
	}

	if _, err := io.Copy(hash, reader); err != nil {
		return "", nil, err
	}

	if sha256 == "" {
		data, err := hash.MarshalBinary()
		if err != nil {
			return "", nil, err
		}
		return "", data, nil
	}
	gotSha256 := hex.EncodeToString(hash.Sum(nil))
	if sha256 != gotSha256 {
		return "", nil, fmt.Errorf("sha256 mismatch: expected %s, got %s", sha256, gotSha256)
	}

	return sha256, nil, nil
}

func (r *ChunkRunner) handlePending(ctx context.Context, chunks []*v1alpha1.Chunk, cb func(*v1alpha1.Chunk)) error {
	size := 0
	for _, chunk := range chunks {
		if chunk.Spec.BearerName != "" {
			bearer, err := r.bearerInformer.Lister().Get(chunk.Spec.BearerName)
			if err == nil {
				if bearer.Status.Phase != v1alpha1.BearerPhaseFailed {
					if bearer.Status.TokenInfo == nil {
						klog.Infof("Bearer %s has no token info, skipping chunk %s", bearer.Name, chunk.Name)
						continue
					}

					expiresIn := bearer.Status.TokenInfo.ExpiresIn
					issuedAt := bearer.Status.TokenInfo.IssuedAt.Time
					if expiresIn > 0 && !issuedAt.IsZero() {
						since := time.Since(issuedAt)
						expires := time.Duration(expiresIn) * time.Second

						if since >= expires {
							if bearer.Status.Phase == v1alpha1.BearerPhaseSucceeded {
								_, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Bearers(), bearer, func(b *v1alpha1.Bearer) *v1alpha1.Bearer {
									b.Status.HandlerName = ""
									b.Status.Phase = v1alpha1.BearerPhasePending
									return b
								})
								if err != nil {
									klog.Warningf("Failed to update bearer %s status: %v", bearer.Name, err)
								}
							}
							klog.Infof("Bearer %s token has expired, skipping chunk %s", bearer.Name, chunk.Name)
							continue
						}
					}
				}
			}
		}
		if chunk.Spec.Sha256PartialPreviousName != "" && chunk.Spec.Sha256PartialPreviousName != "-" {
			pchunk, err := r.chunkInformer.Lister().Get(chunk.Spec.Sha256PartialPreviousName)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					klog.Warningf("Failed to get partial chunk %s: %v", chunk.Spec.Sha256PartialPreviousName, err)
					continue
				}

			} else if pchunk.Status.Phase == v1alpha1.ChunkPhasePending {
				klog.Infof("Partial chunk %s is still pending, skipping chunk %s", chunk.Spec.Sha256PartialPreviousName, chunk.Name)
				continue
			}
		}

		chunk.Status.HandlerName = r.handlerName
		chunk.Status.Phase = v1alpha1.ChunkPhaseRunning
		chunk, err := r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				continue
			}
			klog.Warningf("Failed to acquire chunk %s: %v", chunk.Name, err)
			continue
		}

		if chunk.Status.HandlerName != r.handlerName {
			klog.Infof("Chunk %s was acquired by another handler", chunk.Name)
			continue
		}

		cb(chunk)
		size++

		if size >= r.concurrency {
			break
		}
	}

	if size == 0 {
		return ErrNoPendingChunk
	}

	return nil
}

// getPendingList returns all Chunks in Pending state, sorted by weight and creation time
func (r *ChunkRunner) getPendingList() ([]*v1alpha1.Chunk, error) {
	chunks, err := r.chunkInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		return nil, nil
	}

	var pendingChunks []*v1alpha1.Chunk

	// Filter for Pending state
	for _, chunk := range chunks {
		if chunk.Status.HandlerName == "" && chunk.Status.Phase == v1alpha1.ChunkPhasePending {
			pendingChunks = append(pendingChunks, chunk.DeepCopy())
		}
	}

	// Sort by weight (descending) and creation time (ascending)
	sort.Slice(pendingChunks, func(i, j int) bool {
		a := pendingChunks[i]
		b := pendingChunks[j]
		if a.Spec.Priority != b.Spec.Priority {
			return a.Spec.Priority > b.Spec.Priority
		}

		if a.Status.Retry != b.Status.Retry {
			return a.Status.Retry < b.Status.Retry
		}

		atime := a.CreationTimestamp.Time
		if a.Status.CompletionTime != nil {
			atime = a.Status.CompletionTime.Time
		}
		btime := b.CreationTimestamp.Time
		if b.Status.CompletionTime != nil {
			btime = b.Status.CompletionTime.Time
		}

		return atime.Before(btime)
	})

	return shuffleChunks(pendingChunks), nil
}

// shuffleChunks shuffles the chunks to a certain degree to reduce conflicts
func shuffleChunks(chunks []*v1alpha1.Chunk) []*v1alpha1.Chunk {
	n := len(chunks)
	if n <= 1 {
		return chunks
	}

	// Define a shuffle range (e.g., 25% of the list size)
	shuffleRange := n / 4
	if shuffleRange < 1 {
		shuffleRange = 1
	}

	// Shuffle within the defined range
	for i := 0; i < n; i++ {
		j := i + rand.Intn(shuffleRange)
		if j >= n {
			j = n - 1
		}
		chunks[i], chunks[j] = chunks[j], chunks[i]
	}

	return chunks
}

type hashEncoding interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	hash.Hash
}

func newSha256() hashEncoding {
	return sha256.New().(hashEncoding)
}

type state struct {
	ss  *v1alpha1.Chunk
	mut sync.Mutex
}

func newState(s *v1alpha1.Chunk) *state {
	return &state{
		ss: s.DeepCopy(),
	}
}

func (s *state) Update(fun func(ss *v1alpha1.Chunk) *v1alpha1.Chunk) {
	s.mut.Lock()
	defer s.mut.Unlock()

	status := fun(s.ss.DeepCopy())
	s.ss = status.DeepCopy()
}

func handleProcessError(chunk *v1alpha1.Chunk, typ string, err error) {
	if typ == "" {
		typ = "UnknownError"
	}
	chunk.Status.Retryable = false
	utils.SetChunkTerminalPhase(chunk, v1alpha1.ChunkPhaseFailed)
	chunk.Status.Conditions = v1alpha1.AppendConditions(chunk.Status.Conditions, v1alpha1.Condition{
		Type:    typ,
		Message: err.Error(),
	})
}

func (s *state) handleProcessError(typ string, err error) {
	s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
		handleProcessError(ss, typ, err)
		return ss
	})
}

func (s *state) handleProcessErrorAndRetryable(typ string, err error) {
	s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
		handleProcessError(ss, typ, err)
		if ss.Status.Retry < ss.Spec.MaximumRetry {
			ss.Status.Retryable = true
		}
		return ss
	})
}
