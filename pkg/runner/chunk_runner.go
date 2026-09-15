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
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

var staleResetAfter = 2 * time.Minute

// ChunkRunner executes Chunk tasks
type ChunkRunner struct {
	handlerName    string
	client         versioned.Interface
	chunkInformer  informers.ChunkInformer
	bearerInformer informers.BearerInformer
	httpClient     *http.Client
	signal         chan struct{}
	updateDuration time.Duration
	concurrencySem chan struct{}

	recordMut sync.Mutex
	record    map[string]struct{}
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
		concurrencySem: make(chan struct{}, concurrency),
		record:         map[string]struct{}{},
	}

	r.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			r.enqueueChunk()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			chunk, ok := newObj.(*v1alpha1.Chunk)
			if ok &&
				chunk.Status.HandlerName == "" &&
				chunk.Status.Phase == v1alpha1.ChunkPhasePending {
				r.unmarkRecord(chunk.Name)
			}
			r.enqueueChunk()
		},
		DeleteFunc: func(obj interface{}) {
			chunk, ok := obj.(*v1alpha1.Chunk)
			if ok {
				r.unmarkRecord(chunk.Name)
			}
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

func (r *ChunkRunner) markRecord(name string) bool {
	r.recordMut.Lock()
	defer r.recordMut.Unlock()

	if _, exists := r.record[name]; exists {
		return false
	}

	r.record[name] = struct{}{}
	return true
}

func (r *ChunkRunner) unmarkRecord(name string) {
	r.recordMut.Lock()
	defer r.recordMut.Unlock()

	delete(r.record, name)
}

func (r *ChunkRunner) clearRecord() {
	r.recordMut.Lock()
	defer r.recordMut.Unlock()
	if len(r.record) != 0 {
		r.record = map[string]struct{}{}
	}
}

func (r *ChunkRunner) enqueueChunk() {
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

// Release releases the current held chunk
func (r *ChunkRunner) Release(ctx context.Context) error {
	list, err := r.client.TaskV1alpha1().Chunks().List(ctx, metav1.ListOptions{FieldSelector: "status.handlerName=" + r.handlerName})
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	var wg sync.WaitGroup

	for index := range list.Items {
		chunk := &list.Items[index]
		if !r.holdsChunk(chunk) {
			continue
		}

		klog.Infof("Releasing chunk %s (current phase: %s)", chunk.Name, chunk.Status.Phase)
		wg.Add(1)
		go func(chunk *v1alpha1.Chunk) {
			defer wg.Done()

			_, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Chunks(), chunk, func(chunk *v1alpha1.Chunk) *v1alpha1.Chunk {
				if !r.holdsChunk(chunk) {
					return chunk
				}
				chunk.Status.HandlerName = ""
				chunk.Status.Phase = v1alpha1.ChunkPhasePending
				chunk.Status.Conditions = nil
				return chunk
			})
			if err != nil {
				klog.Errorf("failed to release chunk %s: %v", chunk.Name, err)
			}
		}(chunk)
	}

	wg.Wait()

	return nil
}

func (r *ChunkRunner) holdsChunk(chunk *v1alpha1.Chunk) bool {
	if chunk.Status.HandlerName != r.handlerName {
		return false
	}
	switch chunk.Status.Phase {
	case v1alpha1.ChunkPhasePending, v1alpha1.ChunkPhaseRunning, v1alpha1.ChunkPhaseUnknown:
		return true
	}
	return false
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

	if len(r.concurrencySem) >= cap(r.concurrencySem) {
		select {
		case <-r.signal:
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return false
		}
		return true
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

	stats, err := r.handlePending(context.Background(), chunks, func(c *v1alpha1.Chunk) {
		continues := make(chan struct{})
		go r.process(continues, c.DeepCopy())
		continues <- struct{}{}
		close(continues)

	})
	if err != nil {
		if errors.Is(err, ErrNoPendingChunk) {
			klog.Infof("No chunk acquired: pending=%d free=%d skipped=%d stale=%d conflicts=%d", stats.pending, stats.free, stats.skipped, stats.stale, stats.conflicts)
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

	klog.Infof("Acquired %d/%d chunks: pending=%d conflicts=%d", stats.acquired, stats.free, stats.pending, stats.conflicts)
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
		errMsgs := make([]string, 0, len(bearer.Status.Conditions))
		for _, cond := range bearer.Status.Conditions {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %s", cond.Type, cond.Message))
		}
		return fmt.Errorf("%w: bearer failed phase: %s", ErrAuthentication, strings.Join(errMsgs, "; "))
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
			_, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Bearers(), bearer, func(bearer *v1alpha1.Bearer) *v1alpha1.Bearer {
				bearer.Status.HandlerName = ""
				bearer.Status.Phase = v1alpha1.BearerPhasePending
				return bearer
			})
			if err != nil {
				return err
			}

			return fmt.Errorf("%w: bearer %s token has expired", ErrBearerNotReady, bearer.Name)
		}

		if since >= expires*3/4 {
			_, err := utils.UpdateResourceStatusWithRetry(context.Background(), r.client.TaskV1alpha1().Bearers(), bearer, func(bearer *v1alpha1.Bearer) *v1alpha1.Bearer {
				bearer.Status.HandlerName = ""
				bearer.Status.Phase = v1alpha1.BearerPhasePending
				return bearer
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
			r.unmarkRecord(chunk.Name)
			return nil, 0
		} else if errors.Is(err, ErrAuthentication) {
			s.handleProcessError("AuthenticationError", err)
		} else {
			s.handleProcessError("BearerFetchError", err)
		}
		return nil, 0
	}

	srcReq, err := r.buildRequest(ctx, &chunk.Spec.Source, nil, 0)
	if err != nil {
		retry, err := utils.IsNetworkError(err)
		if retry {
			r.unmarkRecord(chunk.Name)
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
			r.unmarkRecord(chunk.Name)
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
				r.unmarkRecord(chunk.Name)
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
				r.unmarkRecord(chunk.Name)
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

func (r *ChunkRunner) destinationRequest(ctx context.Context, dest *v1alpha1.ChunkHTTP, dr *swmrCount, contentLength int64) (string, bool, error) {
	destReq, err := r.buildRequest(ctx, dest, dr.NewReader(), contentLength)
	if err != nil {
		retry, err := utils.IsNetworkError(err)
		return "", retry, fmt.Errorf("failed to build destination request: %w", err)
	}

	destResp, err := r.httpClient.Do(destReq)
	retry, err := utils.IsHTTPResponseError(destResp, err)
	if err != nil {
		if destResp != nil && destResp.Body != nil {
			destResp.Body.Close()
		}
		return "", retry, fmt.Errorf("failed to perform destination request: %w", err)
	}
	defer destResp.Body.Close()

	if dest.Response.StatusCode != 0 {
		if destResp.StatusCode != dest.Response.StatusCode {
			body, _ := io.ReadAll(destResp.Body)
			return "", false, fmt.Errorf("unexpected status code from destination: got %d, want %d, body: %s",
				destResp.StatusCode, dest.Response.StatusCode, string(body))
		}
	} else {
		if destResp.StatusCode >= http.StatusMultipleChoices {
			body, _ := io.ReadAll(destResp.Body)
			return "", false, fmt.Errorf("destination returned error status code: %d, body: %s", destResp.StatusCode, string(body))
		}
	}

	etag := destResp.Header.Get("ETag")
	if uetag, err := strconv.Unquote(etag); err == nil && uetag != "" {
		etag = uetag
	}

	if etag == "" {
		return "", false, fmt.Errorf("empty ETag received from destination")
	}

	return etag, false, nil
}

func (r *ChunkRunner) process(continues <-chan struct{}, chunk *v1alpha1.Chunk) {
	startTime := time.Now()
	klog.Infof("Processing chunk %s", chunk.Name)
	defer func() {
		duration := time.Since(startTime)
		klog.Infof("Finish processing chunk %s, took %s", chunk.Name, duration)
		<-continues
	}()

	s := newState(chunk)

	var gsr *readCount
	var gdrs []*swmrCount

	ctx, cancel := context.WithCancel(context.Background())

	stopProgress := r.startProgressUpdater(ctx, cancel, s, &gsr, &gdrs)
	defer stopProgress()

	body, contentLength := r.sourceRequest(ctx, chunk, s)
	if body == nil {
		return
	}
	defer body.Close()

	if contentLength > 0 {
		if chunk.Spec.Total > 0 && contentLength != chunk.Spec.Total {
			err := fmt.Errorf("content length mismatch: got %d, want %d", contentLength, chunk.Spec.Total)
			s.handleProcessError("ContentLengthMismatch", err)
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

	g, _ := errgroup.WithContext(ctx)
	sr := newReadCount(ctx, body)
	g.Go(func() error {
		_, err := io.Copy(swmr, sr)
		if closeErr := swmr.Close(); err == nil {
			err = closeErr
		}
		return err
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
		g, _ = errgroup.WithContext(ctx)
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
			var err error
			var etag string
			var retry bool
			waitTime := 5 * time.Second

			for i := 0; i < 5; i++ {
				etag, retry, err = r.destinationRequest(ctx, &dest, dr, contentLength)
				if err == nil {
					break
				}
				if !retry {
					return err
				}
				time.Sleep(waitTime)
				waitTime *= 2
			}
			if err != nil {
				return err
			}
			etags[i] = etag
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		if retry, err := utils.IsNetworkError(err); !retry {
			s.handleProcessError("DestinationRequestError", err)
		} else {
			r.unmarkRecord(chunk.Name)
			s.handleProcessErrorAndRetryable("DestinationRequestError", err)
		}
		return
	}

	if slices.Contains(etags, "") {
		r.unmarkRecord(chunk.Name)
		err := fmt.Errorf("destinations failed to return ETag")
		s.handleProcessErrorAndRetryable("MissingETag", err)
		return
	}

	r.handleSha256AndFinalize(continues, chunk, s, swmr, etags)
}

func (r *ChunkRunner) startProgressUpdater(ctx context.Context, cancel func(), s *state, gsr **readCount, gdrs *[]*swmrCount) func() {
	var (
		prevStatus     *v1alpha1.ChunkStatus
		lastUpdateTime = time.Now()
	)

	chunkFunc := func() {
		s.Update(func(ss *v1alpha1.Chunk) *v1alpha1.Chunk {
			if *gsr != nil {
				updateProgress(&ss.Status, &ss.Spec, *gsr, *gdrs)
			}

			if reflect.DeepEqual(prevStatus, &ss.Status) {
				since := time.Since(lastUpdateTime)
				if since <= staleResetAfter || s.waiting.Load() {
					return ss
				}

				chunk := ss.DeepCopy()
				chunk.Status.Phase = v1alpha1.ChunkPhasePending
				chunk.Status.HandlerName = ""
				_, err := r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
				if err != nil {
					klog.Infof("Failed to reset chunk status for chunk %s: %v", chunk.Name, err)
					return ss
				}
				klog.Infof("Reset chunk status %s", chunk.Name)
				cancel()
				return ss
			}

			chunk, err := r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, ss, metav1.UpdateOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.Infof("Chunk %s not found: %v", ss.Name, err)
					cancel()
					return ss
				}
				if !apierrors.IsConflict(err) {
					klog.Infof("Failed to update chunk status for chunk %s: %v", ss.Name, err)
					return ss
				}

				chunk, err = r.client.TaskV1alpha1().Chunks().Get(ctx, ss.Name, metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						klog.Infof("Chunk %s not found: %v", ss.Name, err)
						cancel()
						return ss
					}
					klog.Infof("Failed to get chunk %s: %v", ss.Name, err)
					return ss
				}
				if chunk.Status.HandlerName != r.handlerName {
					klog.Infof("Chunk %s handler name changed: %v", ss.Name, err)
					cancel()
					return ss
				}

				chunk.Status = ss.Status

				chunk, err = r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						klog.Infof("Chunk %s not found: %v", ss.Name, err)
						cancel()
						return ss
					}
					klog.Infof("Failed to update chunk status for chunk %s: %v", ss.Name, err)
					return ss
				}
			}

			prevStatus = chunk.Status.DeepCopy()
			lastUpdateTime = time.Now()
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

	s.waiting.Store(true)
	<-continues
	r.waitForPartialChunk(s, swmr, etags)
}

func (r *ChunkRunner) waitForPartialChunk(s *state, swmr ioswmr.SWMR, etags []string) {
	chunks := r.client.TaskV1alpha1().Chunks()
	var lastOwnershipCheck time.Time
	for {
		chunk := s.Get()
		if time.Since(lastOwnershipCheck) >= r.updateDuration {
			lastOwnershipCheck = time.Now()
			own, err := chunks.Get(context.Background(), chunk.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				klog.Infof("Chunk %s no longer exists, stop waiting", chunk.Name)
				return
			}
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			if own.Status.HandlerName != r.handlerName {
				klog.Infof("Chunk %s handler name changed", chunk.Name)
				return
			}
		}

		pchunk, err := chunks.Get(context.Background(), chunk.Spec.Sha256PartialPreviousName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
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

type pendingStats struct {
	pending, free, acquired, skipped, stale, conflicts int
}

func (r *ChunkRunner) handlePending(ctx context.Context, chunks []*v1alpha1.Chunk, cb func(*v1alpha1.Chunk)) (pendingStats, error) {
	stats := pendingStats{pending: len(chunks), free: cap(r.concurrencySem) - len(r.concurrencySem)}
	if stats.free <= 0 {
		return stats, nil
	}

	resetBearers := make(map[string]struct{})
	batch := make([]*v1alpha1.Chunk, 0, stats.free)
	flush := func() {
		results := make([]struct {
			chunk *v1alpha1.Chunk
			err   error
		}, len(batch))
		var wg sync.WaitGroup
		for index, chunk := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunk.Status.HandlerName = r.handlerName
				chunk.Status.Phase = v1alpha1.ChunkPhaseRunning
				results[index].chunk, results[index].err = r.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
			}()
		}
		wg.Wait()

		for index, result := range results {
			chunk := batch[index]
			if result.err != nil {
				if apierrors.IsConflict(result.err) || apierrors.IsNotFound(result.err) {
					r.unmarkRecord(chunk.Name)
					stats.conflicts++
					continue
				}
				klog.Warningf("Failed to acquire chunk %s: %v", chunk.Name, result.err)
				r.unmarkRecord(chunk.Name)
				continue
			}

			updated := result.chunk
			if updated.Status.HandlerName != r.handlerName {
				klog.Infof("Chunk %s was acquired by another handler", updated.Name)
				r.unmarkRecord(updated.Name)
				continue
			}

			r.concurrencySem <- struct{}{}
			stats.acquired++
			go func() {
				defer func() {
					<-r.concurrencySem
					r.enqueueChunk()
				}()
				cb(updated)
			}()
		}
		batch = batch[:0]
	}

	for _, chunk := range chunks {
		if stats.acquired >= stats.free {
			break
		}

		if !r.markRecord(chunk.Name) {
			klog.Infof("Chunk %s is already being processed, skipping", chunk.Name)
			stats.skipped++
			continue
		}

		if chunk.Spec.BearerName != "" {
			bearer, err := r.bearerInformer.Lister().Get(chunk.Spec.BearerName)
			if err == nil {
				if bearer.Status.Phase != v1alpha1.BearerPhaseFailed {
					if bearer.Status.TokenInfo == nil {
						klog.Infof("Bearer %s has no token info, skipping chunk %s", bearer.Name, chunk.Name)
						r.unmarkRecord(chunk.Name)
						stats.skipped++
						continue
					}

					expiresIn := bearer.Status.TokenInfo.ExpiresIn
					issuedAt := bearer.Status.TokenInfo.IssuedAt.Time
					if expiresIn > 0 && !issuedAt.IsZero() {
						since := time.Since(issuedAt)
						expires := time.Duration(expiresIn) * time.Second

						if since >= expires {
							if bearer.Status.Phase == v1alpha1.BearerPhaseSucceeded {
								if _, reset := resetBearers[bearer.Name]; !reset {
									resetBearers[bearer.Name] = struct{}{}
									_, err := utils.UpdateResourceStatusWithRetry(ctx, r.client.TaskV1alpha1().Bearers(), bearer, func(bearer *v1alpha1.Bearer) *v1alpha1.Bearer {
										bearer.Status.HandlerName = ""
										bearer.Status.Phase = v1alpha1.BearerPhasePending
										return bearer
									})
									if err != nil {
										klog.Warningf("Failed to update bearer %s status: %v", bearer.Name, err)
									}
								}
							}
							klog.Infof("Bearer %s token has expired, skipping chunk %s", bearer.Name, chunk.Name)
							r.unmarkRecord(chunk.Name)
							stats.skipped++
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
					r.unmarkRecord(chunk.Name)
					stats.skipped++
					continue
				}

			} else if pchunk.Status.Phase == v1alpha1.ChunkPhasePending {
				klog.Infof("Partial chunk %s is still pending, skipping chunk %s", chunk.Spec.Sha256PartialPreviousName, chunk.Name)
				r.unmarkRecord(chunk.Name)
				stats.skipped++
				continue
			}
		}

		latest, err := r.chunkInformer.Lister().Get(chunk.Name)
		if err != nil || latest.Status.HandlerName != "" || latest.Status.Phase != v1alpha1.ChunkPhasePending {
			r.unmarkRecord(chunk.Name)
			stats.stale++
			continue
		}

		batch = append(batch, latest.DeepCopy())
		if len(batch) >= stats.free-stats.acquired {
			flush()
		}
	}
	if len(batch) != 0 {
		flush()
	}

	if stats.acquired == 0 {
		r.clearRecord()
		return stats, ErrNoPendingChunk
	}

	return stats, nil
}

// getPendingList returns pending Chunks by priority descending, retry ascending, and random order within each tier.
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

	sort.Slice(pendingChunks, func(i, j int) bool {
		a := pendingChunks[i]
		b := pendingChunks[j]
		if a.Spec.Priority != b.Spec.Priority {
			return a.Spec.Priority > b.Spec.Priority
		}

		return a.Status.Retry < b.Status.Retry
	})

	for start := 0; start < len(pendingChunks); {
		end := start + 1
		for end < len(pendingChunks) &&
			pendingChunks[end].Spec.Priority == pendingChunks[start].Spec.Priority &&
			pendingChunks[end].Status.Retry == pendingChunks[start].Status.Retry {
			end++
		}
		tier := pendingChunks[start:end]
		rand.Shuffle(len(tier), func(left, right int) {
			tier[left], tier[right] = tier[right], tier[left]
		})
		start = end
	}

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
	ss      *v1alpha1.Chunk
	mut     sync.Mutex
	waiting atomic.Bool
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

func (s *state) Get() *v1alpha1.Chunk {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.ss.DeepCopy()
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
