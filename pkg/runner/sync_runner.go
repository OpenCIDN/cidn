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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
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
	sync, err := r.getPending(context.Background())
	if err != nil {
		select {
		case <-r.signal:
		case <-ctx.Done():
			return false
		}
		return true
	}

	if sync.Spec.HandlerName != r.handlerName {
		return true
	}

	klog.Infof("Processing sync %s (handler: %s)", sync.Name, sync.Spec.HandlerName)
	defer klog.Infof("Finished processing sync %s", sync.Name)
	// Process the sync
	r.process(ctx, sync.Name)
	return true
}

func (r *SyncRunner) updateSync(ctx context.Context, sync *v1alpha1.Sync) (*v1alpha1.Sync, error) {
	now := metav1.Now()
	sync.Status.LastTime = &now
	return r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
}

func (r *SyncRunner) handleProcessErrorWithReason(ctx context.Context, name string, errMsg error, reason string) {
	if errors.Is(errMsg, context.Canceled) {
		err := r.Release(ctx)
		if err != nil {
			klog.Errorf("Error releasing sync: %v", err)
		}
		return
	}
	sync, err := r.syncInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Errorf("Sync %q not found", name)
			return
		}
		klog.Errorf("Error getting sync %q: %v", name, err)
		return
	}

	const typ = "Process"

	sync.Status.Progress = sync.Spec.Total
	sync.Status.Phase = v1alpha1.SyncPhaseFailed
	hasProcessCondition := false
	for _, condition := range sync.Status.Conditions {
		if condition.Type == typ {
			hasProcessCondition = true
			break
		}
	}
	if !hasProcessCondition {
		sync.Status.Conditions = append(sync.Status.Conditions, v1alpha1.Condition{
			Type:               typ,
			Reason:             reason,
			Status:             v1alpha1.ConditionTrue,
			Message:            errMsg.Error(),
			LastTransitionTime: metav1.Now(),
		})
	}
	_, err = r.updateSync(ctx, sync)
	if err != nil {
		klog.Errorf("Error updating sync to failed state: %v", err)
	}
}

func (r *SyncRunner) handleProcessError(ctx context.Context, name string, err error) {
	r.handleProcessErrorWithReason(ctx, name, err, "ProcessFailed")
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

	return sync, nil
}

func (r *SyncRunner) process(ctx context.Context, name string) {
	sync, err := r.getSync(name)
	if err != nil {
		r.handleProcessError(ctx, sync.Name, err)
		return
	}

	srcReq, err := r.buildRequest(ctx, &sync.Spec.Source, nil)
	if err != nil {
		r.handleProcessError(ctx, sync.Name, err)
		return
	}

	srcResp, err := r.httpClient.Do(srcReq)
	if err != nil {
		r.handleProcessError(ctx, sync.Name, err)
		return
	}

	defer srcResp.Body.Close()

	if sync.Spec.Source.Response.StatusCode != 0 {
		if srcResp.StatusCode != sync.Spec.Source.Response.StatusCode {
			err := fmt.Errorf("unexpected status code from source: got %d, want %d",
				srcResp.StatusCode, sync.Spec.Source.Response.StatusCode)
			r.handleProcessError(ctx, sync.Name, err)
			return
		}
	} else {
		if srcResp.StatusCode >= http.StatusMultipleChoices {
			err := fmt.Errorf("source returned error status code: %d", srcResp.StatusCode)
			r.handleProcessError(ctx, sync.Name, err)
			return
		}
	}

	if srcResp.ContentLength != sync.Spec.Total {
		err := fmt.Errorf("content length mismatch: got %d, want %d", srcResp.ContentLength, sync.Spec.Total)
		r.handleProcessErrorWithReason(ctx, sync.Name, err, "ContentLengthMismatch")
		return
	}

	var cleanup func()
	f, err := os.CreateTemp("", "")
	if err == nil {
		defer func() {
			cleanup()
		}()

		cleanup = func() {
			f.Close()
			os.Remove(f.Name())
		}
	}

	swmr := ioswmr.NewSWMR(f)

	g, gctx := errgroup.WithContext(ctx)

	gctx, gcancel := context.WithCancel(gctx)

	sr := NewReadCount(gctx, srcResp.Body)
	g.Go(func() error {
		_, err := io.Copy(swmr, sr)
		if err != nil {
			return err
		}

		return swmr.Close()
	})

	etags := make([]string, len(sync.Spec.Destination))

	drs := []*ReadCount{}

	for i, dest := range sync.Spec.Destination {
		dest := dest
		i := i
		dr := NewReadCount(gctx, swmr.NewReader())
		drs = append(drs, dr)
		g.Go(func() error {
			destReq, err := r.buildRequest(ctx, &dest, dr)
			if err != nil {
				return err
			}

			destResp, err := r.httpClient.Do(destReq)
			if err != nil {
				return err
			}
			defer destResp.Body.Close()

			if dest.Response.StatusCode != 0 {
				if destResp.StatusCode != dest.Response.StatusCode {
					return fmt.Errorf("unexpected status code from destination: got %d, want %d",
						destResp.StatusCode, dest.Response.StatusCode)
				}
			} else {
				if destResp.StatusCode >= http.StatusMultipleChoices {
					body, err := io.ReadAll(destResp.Body)
					if err != nil {
						return fmt.Errorf("destination returned error status code: %d (failed to read response body: %v)", destResp.StatusCode, err)
					}
					return fmt.Errorf("destination returned error status code: %d, body: %s", destResp.StatusCode, string(body))
				}
			}

			etag := destResp.Header.Get("ETag")
			if uetag, err := strconv.Unquote(etag); err == nil && uetag != "" {
				etag = uetag
			}

			if etag == "" {
				return fmt.Errorf("empty ETag received from destination")
			}
			etags[i] = etag
			return nil
		})
	}

	dur := time.Second
	ticker := time.NewTicker(time.Second)

	go func() {
		defer func() {
			gcancel()
		}()
		for range ticker.C {
			cacheSync, err := r.getSync(sync.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return
				}
				klog.Warningf("error getting sync: %v", err)
				continue
			}

			if cacheSync.UID != sync.UID {
				return
			}

			sync := cacheSync.DeepCopy()

			updateProgress(sync, sr, drs)

			_, err = r.updateSync(ctx, sync)
			if err != nil {
				klog.Warningf("error updating sync: %v", err)
				continue
			}

			dur = time.Second + time.Duration(rand.Intn(100))*time.Millisecond
			ticker.Reset(dur)
		}
	}()

	err = g.Wait()
	ticker.Stop()

	if err != nil {
		r.handleProcessError(ctx, sync.Name, err)
		return
	}

	cacheSync, err := r.getSync(sync.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		klog.Warningf("error getting sync: %v", err)
		return
	}

	if cacheSync.UID != sync.UID {
		return
	}

	sync = cacheSync.DeepCopy()

	sync.Status.Etags = etags

	updateProgress(sync, sr, drs)

	if sync.Spec.Sha256PartialPreviousName == "" {
		sync.Status.Phase = v1alpha1.SyncPhaseSucceeded
		_, err := r.updateSync(ctx, sync)
		if err != nil {
			klog.Errorf("Error updating sync to succeeded state: %v", err)
			return
		}
		return
	}

	if sync.Spec.Sha256PartialPreviousName == "-" {
		err := r.processSha256(ctx, sync, swmr, nil)
		if err != nil {
			r.handleProcessError(ctx, sync.Name, err)
			return
		}
		return
	}

	psync, err := r.getSync(sync.Spec.Sha256PartialPreviousName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			r.handleProcessError(ctx, sync.Name, err)
			return
		}
	}
	if psync != nil && psync.Status.Phase == v1alpha1.SyncPhaseSucceeded {
		if len(psync.Status.Sha256Partial) == 0 {
			err := fmt.Errorf("partial sync %q has no sha256 partial data", sync.Spec.Sha256PartialPreviousName)
			if err != nil {
				r.handleProcessErrorWithReason(ctx, sync.Name, err, "MissingSha256PartialData")
			}
			return
		}

		err := r.processSha256(ctx, sync, swmr, psync.Status.Sha256Partial)
		if err != nil {
			r.handleProcessErrorWithReason(ctx, sync.Name, err, "Sha256ProcessingFailed")
			return
		}
		return
	}

	sync, err = r.updateSync(ctx, sync)
	if err != nil {
		klog.Errorf("Error updating sync: %v", err)
		return
	}

	ocleanup := cleanup
	cleanup = func() {}

	go func() {
		defer ocleanup()

		for {
			// TODO: Optimize to use push notifications in the future
			time.Sleep(time.Second)

			psync, err := r.getSync(sync.Spec.Sha256PartialPreviousName)
			if err != nil {
				if apierrors.IsNotFound(err) {
					sync, err := r.getSync(sync.Name)
					if err != nil {
						r.handleProcessError(ctx, sync.Name, err)
						return
					}

					klog.Infof("Partial sync %q not found, waiting for it to be created", sync.Spec.Sha256PartialPreviousName)
					r.updateSync(ctx, sync)
					continue
				}
				r.handleProcessError(ctx, sync.Name, err)
				return
			}

			if psync.Status.Phase != v1alpha1.SyncPhaseSucceeded {
				sync, err := r.getSync(sync.Name)
				if err != nil {
					r.handleProcessError(ctx, sync.Name, err)
					return
				}

				klog.Infof("Partial sync %q is not yet succeeded (current phase: %s), waiting...", sync.Spec.Sha256PartialPreviousName, psync.Status.Phase)
				r.updateSync(ctx, sync)
				continue
			}

			if len(psync.Status.Sha256Partial) == 0 {
				err := fmt.Errorf("partial sync %q has no sha256 partial data", sync.Spec.Sha256PartialPreviousName)
				if err != nil {
					r.handleProcessErrorWithReason(ctx, sync.Name, err, "MissingSha256PartialData")
				}
				return
			}

			sync, err := r.getSync(sync.Name)
			if err != nil {
				r.handleProcessError(ctx, sync.Name, err)
				return
			}

			err = r.processSha256(ctx, sync, swmr, psync.Status.Sha256Partial)
			if err != nil {
				r.handleProcessErrorWithReason(ctx, sync.Name, err, "Sha256ProcessingFailed")
			}
			return
		}
	}()
}

func updateProgress(sync *v1alpha1.Sync, sr *ReadCount, drs []*ReadCount) {
	var progress int64
	sourceProgress := sr.Count()

	progress += sourceProgress

	destinationProgresses := make([]int64, 0, len(sync.Spec.Destination))
	for _, dr := range drs {
		destinationProgress := dr.Count()
		progress += destinationProgress
		destinationProgresses = append(destinationProgresses, destinationProgress)
	}

	sync.Status.Progress = progress / int64(len(sync.Spec.Destination)+1)
	sync.Status.SourceProgress = sourceProgress
	sync.Status.DestinationProgresses = destinationProgresses
}

func (r *SyncRunner) processSha256(ctx context.Context, sync *v1alpha1.Sync, swmr ioswmr.SWMR, sha256Partial []byte) error {
	s := newSha256()

	if len(sha256Partial) > 0 {
		err := s.UnmarshalBinary(sha256Partial)
		if err != nil {
			return err
		}
	}

	if _, err := io.Copy(s, swmr.NewReader()); err != nil {
		return err
	}

	if sync.Spec.Sha256 == "" {
		data, err := s.MarshalBinary()
		if err != nil {
			return err
		}
		sync.Status.Sha256Partial = data
	} else {
		sync.Status.Sha256 = hex.EncodeToString(s.Sum(nil))
		if sync.Spec.Sha256 != sync.Status.Sha256 {
			return fmt.Errorf("sha256 mismatch: expected %s, got %s", sync.Spec.Sha256, sync.Status.Sha256)
		}
	}

	sync.Status.Phase = v1alpha1.SyncPhaseSucceeded
	_, err := r.updateSync(ctx, sync)
	if err != nil {
		klog.Errorf("Error updating sync after sha256 processing: %v", err)
	}
	return nil
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

		ca := a.Spec.ChunksNumber - a.Spec.ChunkIndex
		cb := b.Spec.ChunksNumber - b.Spec.ChunkIndex
		if ca != cb {
			if reflect.DeepEqual(a.Labels, b.Labels) {
				return ca > cb
			}
			return ca < cb
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
