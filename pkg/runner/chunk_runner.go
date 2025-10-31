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

// ChunkRunner executes Chunk tasks
type ChunkRunner struct {
	handlerName    string
	client         versioned.Interface
	chunkInformer  informers.ChunkInformer
	bearerInformer informers.BearerInformer
	httpClient     *http.Client
	signal         chan struct{}
}

// NewChunkRunner creates a new Runner instance
func NewChunkRunner(
	handlerName string,
	clientset versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ChunkRunner {
	r := &ChunkRunner{
		handlerName:    handlerName,
		client:         clientset,
		chunkInformer:  sharedInformerFactory.Task().V1alpha1().Chunks(),
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		httpClient:     http.DefaultClient,
		signal:         make(chan struct{}, 1),
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

			chunkCopy := s.DeepCopy()
			chunkCopy.Status.HandlerName = ""
			chunkCopy.Status.Phase = v1alpha1.ChunkPhasePending
			chunkCopy.Status.Conditions = nil
			_, err := r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunkCopy, metav1.UpdateOptions{})
			if err != nil {
				if apierrors.IsConflict(err) {
					latest, getErr := r.client.TaskV1alpha1().Chunks().Get(ctx, chunkCopy.Name, metav1.GetOptions{})
					if getErr != nil {
						klog.Errorf("failed to get latest chunk %s: %v", chunkCopy.Name, getErr)
						return
					}
					latest.Status.HandlerName = ""
					latest.Status.Phase = v1alpha1.ChunkPhasePending
					latest.Status.Conditions = nil
					_, err = r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, latest, metav1.UpdateOptions{})
					if err != nil {
						klog.Errorf("failed to update chunk %s: %v", latest.Name, err)
						return
					}
				}
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
	s, err := r.getPending(context.Background())
	if err != nil {
		klog.Errorf("failed to get pending chunk: %v", err)

		select {
		case <-r.signal:
		case <-ctx.Done():
			return false
		}
		return true
	}

	if s.Status.HandlerName != r.handlerName {
		return true
	}

	continues := make(chan struct{})

	go r.process(context.Background(), s.DeepCopy(), continues)
	continues <- struct{}{}
	close(continues)

	return true
}

func (r *ChunkRunner) updateChunk(ctx context.Context, chunk *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
	return r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
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
	req.ContentLength = contentLength

	// Add custom headers from configuration
	for k, v := range chunkHTTP.Request.Headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

func (r *ChunkRunner) getChunk(name string) (*v1alpha1.Chunk, error) {
	chunk, err := r.chunkInformer.Lister().Get(name)
	if err != nil {
		return nil, err
	}

	return chunk.DeepCopy(), nil
}

// tryAddBearer fetches the bearer token and adds the Authorization header to the chunk
// Returns shouldWait=true when bearer token is not ready or has expired, requiring the chunk
// to wait without consuming retry attempts, and err for actual errors.
func (r *ChunkRunner) tryAddBearer(ctx context.Context, chunk *v1alpha1.Chunk) (bool, error) {
	if chunk.Spec.BearerName == "" {
		return false, nil
	}
	bearer, err := r.bearerInformer.Lister().Get(chunk.Spec.BearerName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if bearer != nil {
		if bearer.Status.TokenInfo == nil {
			if bearer.Status.Phase == v1alpha1.BearerPhaseSucceeded {
				return false, fmt.Errorf("bearer %s is in succeeded phase but has no token info", bearer.Name)
			}
			// Bearer is not in succeeded phase yet, release chunk to pending and wait for bearer to complete
			return true, nil
		}

		if bearer.Status.TokenInfo.Token != "" {
			if chunk.Spec.Source.Request.Headers == nil {
				chunk.Spec.Source.Request.Headers = make(map[string]string)
			}
			chunk.Spec.Source.Request.Headers["Authorization"] = "Bearer " + bearer.Status.TokenInfo.Token

			issuedAt := bearer.Status.TokenInfo.IssuedAt.Time
			expiresIn := bearer.Status.TokenInfo.ExpiresIn
			since := time.Since(issuedAt)
			expires := time.Duration(expiresIn) * time.Second

			if since >= expires {
				bearer = bearer.DeepCopy()
				bearer.Status.HandlerName = ""
				bearer.Status.Phase = v1alpha1.BearerPhasePending
				_, err := r.client.TaskV1alpha1().Bearers().UpdateStatus(ctx, bearer, metav1.UpdateOptions{})
				if err != nil {
					return false, err
				}

				// Bearer token has expired, release chunk to pending and wait for bearer refresh cycle to complete
				return true, nil
			}

			if since >= expires*3/4 {
				bearer = bearer.DeepCopy()
				bearer.Status.HandlerName = ""
				bearer.Status.Phase = v1alpha1.BearerPhasePending

				_, err := r.client.TaskV1alpha1().Bearers().UpdateStatus(context.Background(), bearer, metav1.UpdateOptions{})
				if err != nil {
					klog.Errorf("Failed to update bearer %s status: %v", bearer.Name, err)
				}
			}
		}
	}

	return false, nil
}

func (r *ChunkRunner) sourceRequest(ctx context.Context, chunk *v1alpha1.Chunk, s *state) (io.ReadCloser, int64) {
	shouldWait, err := r.tryAddBearer(ctx, chunk)
	if shouldWait {
		// Bearer token is not ready or has expired, release chunk to pending
		klog.Infof("Bearer token not ready for chunk %s, releasing to pending", chunk.Name)
		s.handleReleaseToPending()
		return nil, 0
	}
	if err != nil {
		s.handleProcessErrorAndRetryable("", err)
		return nil, 0
	}

	srcReq, err := r.buildRequest(ctx, &chunk.Spec.Source, nil, 0)
	if err != nil {
		retry, err := utils.IsNetWorkError(err)
		if retry {
			s.handleProcessErrorAndRetryable("", err)
		} else {
			s.handleProcessError("", err)
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
			s.handleProcessErrorAndRetryable("", err)
		} else {
			s.handleProcessError("", err)
		}
		return nil, 0
	}

	headers := map[string]string{}
	for k := range srcResp.Header {
		headers[strings.ToLower(k)] = srcResp.Header.Get(k)
	}

	s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
		ss.Status.SourceResponse = &v1alpha1.ChunkHTTPResponse{
			StatusCode: srcResp.StatusCode,
			Headers:    headers,
		}

		return ss, nil
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

				s.handleProcessError("", err)
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
				s.handleProcessError("", err)
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

func (r *ChunkRunner) destinationRequest(ctx context.Context, dest *v1alpha1.ChunkHTTP, dr io.Reader, contentLength int64) (string, error) {
	destReq, err := r.buildRequest(ctx, dest, dr, contentLength)
	if err != nil {
		if retry, err := utils.IsNetWorkError(err); !retry {
			return "", err
		}

		destReq, err = r.buildRequest(ctx, dest, dr, contentLength)
		if err != nil {
			return "", err
		}
	}

	destResp, err := r.httpClient.Do(destReq)
	if err != nil {
		if retry, err := utils.IsHTTPResponseError(destResp, err); !retry {
			return "", err
		}

		destResp, err = r.httpClient.Do(destReq)
		if err != nil {
			return "", err
		}
	}
	defer destResp.Body.Close()

	if dest.Response.StatusCode != 0 {
		if destResp.StatusCode != dest.Response.StatusCode {
			return "", fmt.Errorf("unexpected status code from destination: got %d, want %d",
				destResp.StatusCode, dest.Response.StatusCode)
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

func (r *ChunkRunner) process(ctx context.Context, chunk *v1alpha1.Chunk, continues <-chan struct{}) {
	klog.Infof("Processing chunk %s (handler: %s)", chunk.Name, chunk.Status.HandlerName)
	defer klog.Infof("Finish processing chunk %s", chunk.Name)
	defer func() { <-continues }()

	s := newState(chunk)

	var gsr *ReadCount
	var gdrs []*ReadCount
	ctx, cancel := context.WithCancel(ctx)
	stopProgress := r.startProgressUpdater(ctx, func() {
		cancel()
		<-continues
	}, s, &gsr, &gdrs)
	defer stopProgress()

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
		s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
			if chunk.Spec.InlineResponseBody {
				body, err := io.ReadAll(body)
				if err != nil {
					return nil, err
				}
				ss.Status.ResponseBody = body
			}
			ss.Status.Phase = v1alpha1.ChunkPhaseSucceeded
			return ss, nil
		})
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

	g, _ := errgroup.WithContext(ctx)

	sr := NewReadCount(ctx, body)
	g.Go(func() error {
		_, err := io.Copy(swmr, sr)
		if err != nil {
			return err
		}
		return swmr.Close()
	})

	etags := make([]string, len(chunk.Spec.Destination))
	drs := make([]*ReadCount, 0, len(chunk.Spec.Destination))

	for _, dest := range chunk.Spec.Destination {
		dest := dest
		if dest.Request.Method == "" {
			continue
		}
		dr := NewReadCount(ctx, swmr.NewReader())
		drs = append(drs, dr)
	}

	s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
		gsr = sr
		gdrs = drs

		return ss, nil
	})

	if contentLength <= 0 {
		err = g.Wait()
		if err != nil {
			s.handleProcessError("", err)
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
		s.handleProcessError("", err)
		return
	}

	r.handleSha256AndFinalize(ctx, chunk, s, swmr, etags, continues)
}

func (r *ChunkRunner) startProgressUpdater(ctx context.Context, cancel func(), s *state, gsr **ReadCount, gdrs *[]*ReadCount) func() {
	chunkFunc := func() {
		s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {

			if *gsr != nil {
				updateProgress(&ss.Status, &ss.Spec, *gsr, *gdrs)
			}

			newChunk, err := r.updateChunk(ctx, ss)
			if err != nil {
				if !apierrors.IsConflict(err) {
					klog.Warningf("Failed to update chunk %s: %v", ss.Name, err)
					return ss, nil
				}
				newChunk, err = r.getChunk(ss.Name)
				if err != nil {
					if apierrors.IsNotFound(err) {
						cancel()
						klog.Warningf("Chunk %s not found, may have been deleted", ss.Name)
						return ss, nil
					}
					klog.Warningf("Failed to get chunk %s: %v", ss.Name, err)
					return ss, nil
				}

				if newChunk.Status.HandlerName != r.handlerName {
					cancel()
					klog.Warningf("Chunk %s has been acquired by another handler %s", ss.Name, newChunk.Status.HandlerName)
					return ss, nil
				}
				newChunk.Status = ss.Status
				newChunk, err = r.updateChunk(ctx, newChunk)
				if err != nil {
					klog.Warningf("Failed to update chunk %s after retry: %v", ss.Name, err)
					return ss, nil
				}
			}
			return newChunk, nil
		})
	}

	dur := time.Second
	ticker := time.NewTicker(dur)
	stop := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				chunkFunc()
				dur = time.Second + time.Duration(rand.Intn(100))*time.Millisecond
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

func (r *ChunkRunner) handleSha256AndFinalize(ctx context.Context, chunk *v1alpha1.Chunk, s *state, swmr ioswmr.SWMR, etags []string, continues <-chan struct{}) {
	if chunk.Spec.Sha256PartialPreviousName == "" {
		s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
			ss.Status.Etags = etags
			ss.Status.Phase = v1alpha1.ChunkPhaseSucceeded
			return ss, nil
		})
		return
	}
	if chunk.Spec.Sha256PartialPreviousName == "-" {
		s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
			var err error
			ss.Status.Sha256, ss.Status.Sha256Partial, err = updateSha256(ss.Spec.Sha256, nil, swmr.NewReader())
			if err != nil {
				return nil, err
			}
			ss.Status.Etags = etags
			ss.Status.Phase = v1alpha1.ChunkPhaseSucceeded
			return ss, nil
		})
		return
	}
	pchunk, err := r.getChunk(chunk.Spec.Sha256PartialPreviousName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			s.handleProcessError("", err)
			return
		}
	} else {
		if pchunk.Status.Phase == v1alpha1.ChunkPhaseSucceeded {
			if len(pchunk.Status.Sha256Partial) == 0 {
				err := fmt.Errorf("partial chunk %q has no sha256 partial data", chunk.Spec.Sha256PartialPreviousName)
				s.handleProcessError("MissingSha256PartialData", err)
				return
			}
			s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
				var err error
				ss.Status.Sha256, ss.Status.Sha256Partial, err = updateSha256(ss.Spec.Sha256, pchunk.Status.Sha256Partial, swmr.NewReader())
				if err != nil {
					return nil, err
				}
				ss.Status.Etags = etags
				ss.Status.Phase = v1alpha1.ChunkPhaseSucceeded
				return ss, nil
			})
			return
		}
	}

	<-continues
	r.waitForPartialChunk(ctx, chunk, s, swmr, etags)
}

func (r *ChunkRunner) waitForPartialChunk(ctx context.Context, chunk *v1alpha1.Chunk, s *state, swmr ioswmr.SWMR, etags []string) {
	for {
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
		pchunk, err := r.getChunk(chunk.Spec.Sha256PartialPreviousName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				s.handleProcessError("", err)
				return
			}
			continue
		}
		if pchunk.Status.Phase != v1alpha1.ChunkPhaseSucceeded {
			continue
		}
		if len(pchunk.Status.Sha256Partial) == 0 {
			err := fmt.Errorf("partial chunk %q has no sha256 partial data", chunk.Spec.Sha256PartialPreviousName)
			s.handleProcessError("MissingSha256PartialData", err)
			return
		}
		s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
			var err error
			ss.Status.Sha256, ss.Status.Sha256Partial, err = updateSha256(ss.Spec.Sha256, pchunk.Status.Sha256Partial, swmr.NewReader())
			if err != nil {
				return nil, err
			}
			ss.Status.Etags = etags
			ss.Status.Phase = v1alpha1.ChunkPhaseSucceeded
			return ss, nil
		})
		return
	}
}

func updateProgress(status *v1alpha1.ChunkStatus, spec *v1alpha1.ChunkSpec, sr *ReadCount, drs []*ReadCount) {
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

func (r *ChunkRunner) getPending(ctx context.Context) (*v1alpha1.Chunk, error) {
	chunks, err := r.getPendingList()
	if err != nil {
		return nil, err
	}

	for _, chunk := range chunks {
		chunk.Status.HandlerName = r.handlerName
		chunk.Status.Phase = v1alpha1.ChunkPhaseRunning

		chunk, err := r.updateChunk(ctx, chunk)
		if err != nil {
			if apierrors.IsConflict(err) {
				// Someone else got the chunk first, try next one
				continue
			}
			return nil, err
		}

		// Successfully acquired the chunk
		return chunk, nil
	}

	// No pending chunks available
	return nil, fmt.Errorf("no pending chunks available")
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

		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	})

	return pendingChunks, nil
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

func (s *state) Update(fun func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error)) {
	s.mut.Lock()
	defer s.mut.Unlock()

	status, err := fun(s.ss.DeepCopy())
	if err != nil {
		handleProcessError(&s.ss.Status, "", err)
	} else {
		s.ss = status.DeepCopy()
	}
}

func handleProcessError(ss *v1alpha1.ChunkStatus, typ string, err error) {
	if typ == "" {
		typ = "Process"
	}
	ss.Phase = v1alpha1.ChunkPhaseFailed
	ss.Conditions = v1alpha1.AppendConditions(ss.Conditions, v1alpha1.Condition{
		Type:    typ,
		Message: err.Error(),
	})
}

func (s *state) handleProcessError(typ string, err error) {
	s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
		handleProcessError(&ss.Status, typ, err)
		ss.Status.Retryable = false
		return ss, nil
	})
}

func (s *state) handleProcessErrorAndRetryable(typ string, err error) {
	s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
		handleProcessError(&ss.Status, typ, err)
		if ss.Status.Retry < ss.Spec.MaximumRetry {
			ss.Status.Retryable = true
		} else {
			ss.Status.Retryable = false
		}
		return ss, nil
	})
}

// handleReleaseToPending releases the chunk back to pending phase without consuming retry attempts,
// typically used when waiting for external dependencies like bearer tokens.
// This method clears the phase, handler name, and conditions while preserving the retry count,
// which is the key difference from failure-based phase transitions.
func (s *state) handleReleaseToPending() {
	s.Update(func(ss *v1alpha1.Chunk) (*v1alpha1.Chunk, error) {
		ss.Status.Phase = v1alpha1.ChunkPhasePending
		ss.Status.HandlerName = ""
		ss.Status.Conditions = nil
		// Retry count is intentionally preserved
		return ss, nil
	})
}
