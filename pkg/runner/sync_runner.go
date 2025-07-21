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

// SyncRunner executes Sync tasks
type SyncRunner struct {
	handlerName  string
	client       versioned.Interface
	syncInformer informers.SyncInformer
	httpClient   *http.Client
	signal       chan struct{}
}

// NewSyncRunner creates a new Runner instance
func NewSyncRunner(
	handlerName string,
	clientset versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *SyncRunner {
	r := &SyncRunner{
		handlerName:  handlerName,
		client:       clientset,
		syncInformer: sharedInformerFactory.Task().V1alpha1().Syncs(),
		httpClient:   http.DefaultClient,
		signal:       make(chan struct{}, 1),
	}

	r.syncInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			r.enqueueSync()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			r.enqueueSync()
		},
	})

	return r
}

func (r *SyncRunner) enqueueSync() {
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

// Release releases the current held sync
func (r *SyncRunner) Release(ctx context.Context) error {
	syncs, err := r.syncInformer.Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	var wg sync.WaitGroup

	for _, sync := range syncs {
		if sync.Spec.HandlerName != r.handlerName {
			continue
		}

		if sync.Status.Phase != v1alpha1.SyncPhasePending && sync.Status.Phase != v1alpha1.SyncPhaseRunning {
			continue
		}

		klog.Infof("Releasing sync %s (current phase: %s)", sync.Name, sync.Status.Phase)
		wg.Add(1)
		go func(s *v1alpha1.Sync) {
			defer wg.Done()

			syncCopy := s.DeepCopy()
			syncCopy.Spec.HandlerName = ""
			syncCopy.Status.Phase = v1alpha1.SyncPhasePending
			syncCopy.Status.Conditions = nil
			_, err := r.client.TaskV1alpha1().Syncs().Update(ctx, syncCopy, metav1.UpdateOptions{})
			if err != nil {
				if apierrors.IsConflict(err) {
					latest, getErr := r.client.TaskV1alpha1().Syncs().Get(ctx, syncCopy.Name, metav1.GetOptions{})
					if getErr != nil {
						klog.Errorf("failed to get latest sync %s: %v", syncCopy.Name, getErr)
						return
					}
					latest.Spec.HandlerName = ""
					latest.Status.Phase = v1alpha1.SyncPhasePending
					latest.Status.Conditions = nil
					_, err = r.client.TaskV1alpha1().Syncs().Update(ctx, latest, metav1.UpdateOptions{})
					if err != nil {
						klog.Errorf("failed to update sync %s: %v", latest.Name, err)
						return
					}
				}
				klog.Errorf("failed to release sync %s: %v", s.Name, err)
			}
		}(sync)
	}

	return nil
}

// Shutdown stops the runner
func (r *SyncRunner) Shutdown(ctx context.Context) error {
	return r.Release(ctx)
}

// Run starts the runner
func (r *SyncRunner) Start(ctx context.Context) error {
	go r.runWorker(ctx)

	return nil
}

func (r *SyncRunner) runWorker(ctx context.Context) {
	for r.processNextItem(ctx) {
	}
}

func (r *SyncRunner) processNextItem(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	s, err := r.getPending(context.Background())
	if err != nil {
		select {
		case <-r.signal:
		case <-ctx.Done():
			return false
		}
		return true
	}

	if s.Spec.HandlerName != r.handlerName {
		return true
	}

	continues := make(chan struct{})

	go r.process(context.Background(), s.DeepCopy(), continues)
	continues <- struct{}{}
	close(continues)

	return true
}

func (r *SyncRunner) updateSync(ctx context.Context, sync *v1alpha1.Sync) (*v1alpha1.Sync, error) {
	return r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
}

// buildRequest constructs an HTTP request from SyncHTTP configuration
func (r *SyncRunner) buildRequest(ctx context.Context, syncHTTP *v1alpha1.SyncHTTP, body io.Reader) (*http.Request, error) {

	req, err := http.NewRequestWithContext(ctx, syncHTTP.Request.Method, syncHTTP.Request.URL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Set default headers
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", versions.DefaultUserAgent())

	// Add custom headers from configuration
	for k, v := range syncHTTP.Request.Headers {
		req.Header.Set(k, v)
	}

	if body != nil && req.ContentLength == 0 {
		if cl := req.Header.Get("Content-Length"); cl != "" {
			contentLength, err := strconv.ParseInt(cl, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid Content-Length header: %v", err)
			}
			req.ContentLength = contentLength
		}
	}

	return req, nil
}

func (r *SyncRunner) getSync(name string) (*v1alpha1.Sync, error) {
	sync, err := r.syncInformer.Lister().Get(name)
	if err != nil {
		return nil, err
	}

	return sync.DeepCopy(), nil
}

func (r *SyncRunner) sourceRequest(ctx context.Context, sync *v1alpha1.Sync, s *state) io.ReadCloser {
	srcReq, err := r.buildRequest(ctx, &sync.Spec.Source, nil)
	if err != nil {
		retry, err := utils.IsNetWorkError(err)
		if retry {
			s.handleProcessErrorAndRetryable("", err)
		} else {
			s.handleProcessError("", err)
		}
		return nil
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
		return nil
	}

	if sync.Spec.Source.Response.StatusCode != 0 {
		if srcResp.StatusCode != sync.Spec.Source.Response.StatusCode {
			err := fmt.Errorf("unexpected status code: got %d, want %d",
				srcResp.StatusCode, sync.Spec.Source.Response.StatusCode)

			s.handleProcessError("", err)

			if srcResp.Body != nil {
				srcResp.Body.Close()
			}
			return nil
		}
	} else {
		if srcResp.StatusCode >= http.StatusMultipleChoices {
			err := fmt.Errorf("source returned error status code: %d", srcResp.StatusCode)
			s.handleProcessError("", err)

			if srcResp.Body != nil {
				srcResp.Body.Close()
			}
			return nil
		}
	}

	if srcResp.ContentLength != sync.Spec.Total {
		err := fmt.Errorf("content length mismatch: got %d, want %d", srcResp.ContentLength, sync.Spec.Total)
		s.handleProcessError("ContentLengthMismatch", err)

		if srcResp.Body != nil {
			srcResp.Body.Close()
		}
		return nil
	}

	for k, v := range sync.Spec.Source.Response.Headers {
		respVal := srcResp.Header.Get(k)
		if respVal != v {
			err := fmt.Errorf("header %s mismatch: got %s, want %s", k, respVal, v)
			s.handleProcessError("HeaderMismatch", err)

			if srcResp.Body != nil {
				srcResp.Body.Close()
			}
			return nil
		}
	}

	return srcResp.Body
}

func (r *SyncRunner) destinationRequest(ctx context.Context, dest *v1alpha1.SyncHTTP, dr io.Reader) (string, error) {
	destReq, err := r.buildRequest(ctx, dest, dr)
	if err != nil {
		return "", err
	}

	destResp, err := r.httpClient.Do(destReq)
	if err != nil {
		return "", err
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

func (r *SyncRunner) process(ctx context.Context, sync *v1alpha1.Sync, continues <-chan struct{}) {
	klog.Infof("Processing sync %s (handler: %s)", sync.Name, sync.Spec.HandlerName)
	defer klog.Infof("Finish processing sync %s", sync.Name)
	defer func() { <-continues }()

	s := newState(sync)

	var gsr *ReadCount
	var gdrs []*ReadCount
	ctx, cancel := context.WithCancel(ctx)
	stopProgress := r.startProgressUpdater(ctx, func() {
		cancel()
		<-continues
	}, s, &gsr, &gdrs)
	defer stopProgress()

	body := r.sourceRequest(ctx, sync, s)
	if body == nil {
		return
	}

	f, err := os.CreateTemp("", "cidn-sync-")
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

	etags := make([]string, len(sync.Spec.Destination))
	drs := make([]*ReadCount, 0, len(sync.Spec.Destination))
	for i, dest := range sync.Spec.Destination {
		dest := dest
		i := i
		dr := NewReadCount(ctx, swmr.NewReader())
		drs = append(drs, dr)
		g.Go(func() error {
			etag, err := r.destinationRequest(ctx, &dest, dr)
			if err != nil {

				return err
			}
			etags[i] = etag
			return nil
		})
	}

	s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
		gsr = sr
		gdrs = drs

		return ss, nil
	})

	err = g.Wait()
	if err != nil {
		s.handleProcessErrorAndRetryable("", err)
		return
	}

	r.handleSha256AndFinalize(ctx, sync, s, swmr, etags, continues)
}

func (r *SyncRunner) startProgressUpdater(ctx context.Context, cancel func(), s *state, gsr **ReadCount, gdrs *[]*ReadCount) func() {
	syncFunc := func() {
		s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {

			if *gsr != nil {
				updateProgress(&ss.Status, &ss.Spec, *gsr, *gdrs)
			}

			newSync, err := r.updateSync(ctx, ss)
			if err != nil {
				if !apierrors.IsConflict(err) {
					klog.Warningf("Failed to update sync %s: %v", ss.Name, err)
					return ss, nil
				}
				newSync, err = r.getSync(ss.Name)
				if err != nil {
					if apierrors.IsNotFound(err) {
						cancel()
						klog.Warningf("Sync %s not found, may have been deleted", ss.Name)
						return ss, nil
					}
					klog.Warningf("Failed to get sync %s: %v", ss.Name, err)
					return ss, nil
				}

				if newSync.Spec.HandlerName != r.handlerName {
					cancel()
					klog.Warningf("Sync %s has been acquired by another handler %s", ss.Name, newSync.Spec.HandlerName)
					return ss, nil
				}
				newSync.Status = ss.Status
				newSync, err = r.updateSync(ctx, newSync)
				if err != nil {
					klog.Warningf("Failed to update sync %s after retry: %v", ss.Name, err)
					return ss, nil
				}
			}
			return newSync, nil
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
				syncFunc()
				dur = time.Second + time.Duration(rand.Intn(100))*time.Millisecond
				ticker.Reset(dur)
			case <-stop:
				syncFunc()
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() { close(stop) }
}

func (r *SyncRunner) handleSha256AndFinalize(ctx context.Context, sync *v1alpha1.Sync, s *state, swmr ioswmr.SWMR, etags []string, continues <-chan struct{}) {
	if sync.Spec.Sha256PartialPreviousName == "" {
		s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
			ss.Status.Etags = etags
			ss.Status.Phase = v1alpha1.SyncPhaseSucceeded
			return ss, nil
		})
		return
	}
	if sync.Spec.Sha256PartialPreviousName == "-" {
		s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
			var err error
			ss.Status.Sha256, ss.Status.Sha256Partial, err = updateSha256(ss.Spec.Sha256, nil, swmr.NewReader())
			if err != nil {
				return nil, err
			}
			ss.Status.Etags = etags
			ss.Status.Phase = v1alpha1.SyncPhaseSucceeded
			return ss, nil
		})
		return
	}
	psync, err := r.getSync(sync.Spec.Sha256PartialPreviousName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			s.handleProcessError("", err)
			return
		}
	} else {
		if psync.Status.Phase == v1alpha1.SyncPhaseSucceeded {
			if len(psync.Status.Sha256Partial) == 0 {
				err := fmt.Errorf("partial sync %q has no sha256 partial data", sync.Spec.Sha256PartialPreviousName)
				s.handleProcessError("MissingSha256PartialData", err)
				return
			}
			s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
				var err error
				ss.Status.Sha256, ss.Status.Sha256Partial, err = updateSha256(ss.Spec.Sha256, psync.Status.Sha256Partial, swmr.NewReader())
				if err != nil {
					return nil, err
				}
				ss.Status.Etags = etags
				ss.Status.Phase = v1alpha1.SyncPhaseSucceeded
				return ss, nil
			})
			return
		}
	}

	<-continues
	r.waitForPartialSync(ctx, sync, s, swmr, etags)
}

func (r *SyncRunner) waitForPartialSync(ctx context.Context, sync *v1alpha1.Sync, s *state, swmr ioswmr.SWMR, etags []string) {
	for {
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
		psync, err := r.getSync(sync.Spec.Sha256PartialPreviousName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				s.handleProcessError("", err)
				return
			}
			continue
		}
		if psync.Status.Phase != v1alpha1.SyncPhaseSucceeded {
			continue
		}
		if len(psync.Status.Sha256Partial) == 0 {
			err := fmt.Errorf("partial sync %q has no sha256 partial data", sync.Spec.Sha256PartialPreviousName)
			s.handleProcessError("MissingSha256PartialData", err)
			return
		}
		s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
			var err error
			ss.Status.Sha256, ss.Status.Sha256Partial, err = updateSha256(ss.Spec.Sha256, psync.Status.Sha256Partial, swmr.NewReader())
			if err != nil {
				return nil, err
			}
			ss.Status.Etags = etags
			ss.Status.Phase = v1alpha1.SyncPhaseSucceeded
			return ss, nil
		})
		return
	}
}

func updateProgress(status *v1alpha1.SyncStatus, spec *v1alpha1.SyncSpec, sr *ReadCount, drs []*ReadCount) {
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

func (r *SyncRunner) getPending(ctx context.Context) (*v1alpha1.Sync, error) {
	syncs, err := r.getPendingList()
	if err != nil {
		return nil, err
	}

	for _, sync := range syncs {
		sync.Spec.HandlerName = r.handlerName
		sync.Status.Phase = v1alpha1.SyncPhaseRunning

		sync, err := r.updateSync(ctx, sync)
		if err != nil {
			if apierrors.IsConflict(err) {
				// Someone else got the sync first, try next one
				continue
			}
			return nil, err
		}

		// Successfully acquired the sync
		return sync, nil
	}

	// No pending syncs available
	return nil, fmt.Errorf("no pending syncs available")
}

// getPendingList returns all Syncs in Pending state, sorted by weight and creation time
func (r *SyncRunner) getPendingList() ([]*v1alpha1.Sync, error) {
	syncs, err := r.syncInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}

	if len(syncs) == 0 {
		return nil, nil
	}

	var pendingSyncs []*v1alpha1.Sync

	// Filter for Pending state
	for _, sync := range syncs {
		if sync.Spec.HandlerName == "" && sync.Status.Phase == v1alpha1.SyncPhasePending {
			pendingSyncs = append(pendingSyncs, sync)
		}
	}

	// Sort by weight (descending) and creation time (ascending)
	sort.Slice(pendingSyncs, func(i, j int) bool {
		a := pendingSyncs[i]
		b := pendingSyncs[j]
		if a.Spec.Priority != b.Spec.Priority {
			return a.Spec.Priority > b.Spec.Priority
		}

		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	})

	return pendingSyncs, nil
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
	ss  *v1alpha1.Sync
	mut sync.Mutex
}

func newState(s *v1alpha1.Sync) *state {
	return &state{
		ss: s.DeepCopy(),
	}
}

func (s *state) Update(fun func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error)) {
	s.mut.Lock()
	defer s.mut.Unlock()

	status, err := fun(s.ss.DeepCopy())
	if err != nil {
		handleProcessError(&s.ss.Status, "", err)
	} else {
		s.ss = status.DeepCopy()
	}
}

func handleProcessError(ss *v1alpha1.SyncStatus, typ string, err error) {
	if typ == "" {
		typ = "Process"
	}
	ss.Phase = v1alpha1.SyncPhaseFailed
	ss.Conditions = v1alpha1.AppendConditions(ss.Conditions, v1alpha1.Condition{
		Type:    typ,
		Message: err.Error(),
	})
}

func (s *state) handleProcessError(typ string, err error) {
	s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
		handleProcessError(&ss.Status, typ, err)
		return ss, nil
	})
}

func (s *state) handleProcessErrorAndRetryable(typ string, err error) {
	s.Update(func(ss *v1alpha1.Sync) (*v1alpha1.Sync, error) {
		handleProcessError(&ss.Status, typ, err)

		if ss.Spec.ChunksNumber > 1 {
			ss.Status.Conditions = v1alpha1.AppendConditions(ss.Status.Conditions,
				v1alpha1.Condition{
					Type:    v1alpha1.ConditionTypeRetryable,
					Message: fmt.Sprintf("Retryable, Retry count: %d, chunk: %d/%d", ss.Status.RetryCount, ss.Spec.ChunkIndex, ss.Spec.ChunksNumber),
				},
			)
		} else {
			ss.Status.Conditions = v1alpha1.AppendConditions(ss.Status.Conditions,
				v1alpha1.Condition{
					Type:    v1alpha1.ConditionTypeRetryable,
					Message: fmt.Sprintf("Retryable, Retry count: %d", ss.Status.RetryCount),
				},
			)
		}

		return ss, nil
	})
}
