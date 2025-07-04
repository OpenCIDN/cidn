package runner

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
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
	handlerName           string
	client                versioned.Interface
	sharedInformerFactory externalversions.SharedInformerFactory
	syncInformer          informers.SyncInformer
	httpClient            *http.Client
	signal                chan struct{}
}

// NewSyncRunner creates a new Runner instance
func NewSyncRunner(handlerName string, clientset versioned.Interface) *SyncRunner {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(clientset, 0)
	r := &SyncRunner{
		handlerName:           handlerName,
		client:                clientset,
		sharedInformerFactory: sharedInformerFactory,
		syncInformer:          sharedInformerFactory.Task().V1alpha1().Syncs(),
		httpClient:            http.DefaultClient,
		signal:                make(chan struct{}, 1),
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

// Run starts the runner
func (r *SyncRunner) Start(ctx context.Context) error {
	r.sharedInformerFactory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), r.syncInformer.Informer().HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	go r.runWorker(ctx)

	return nil
}

func (r *SyncRunner) runWorker(ctx context.Context) {
	for r.processNextItem(ctx) {
	}
}

func (r *SyncRunner) processNextItem(ctx context.Context) bool {
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

	// Process the sync
	r.process(ctx, sync)
	return true
}

func (r *SyncRunner) handleProcessError(ctx context.Context, sync *v1alpha1.Sync, err error) {
	klog.Errorf("Error processing sync %s: %v", sync.Name, err)
	sync.Status.Progress = sync.Spec.Total
	sync.Status.Phase = v1alpha1.SyncPhaseFailed
	hasProcessCondition := false
	for _, condition := range sync.Status.Conditions {
		if condition.Type == "Process" {
			hasProcessCondition = true
			break
		}
	}
	if !hasProcessCondition {
		sync.Status.Conditions = append(sync.Status.Conditions, v1alpha1.Condition{
			Type:               "Process",
			Status:             v1alpha1.ConditionTrue,
			Reason:             "ProcessFailed",
			Message:            err.Error(),
			LastTransitionTime: metav1.Now(),
		})
	}
	_, updateErr := r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
	if updateErr != nil {
		klog.Errorf("Error updating sync to failed state: %v", updateErr)
	}
}

// buildRequest constructs an HTTP request from SyncHTTP configuration
func (r *SyncRunner) buildRequest(syncHTTP *v1alpha1.SyncHTTP, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(syncHTTP.Request.Method, syncHTTP.Request.URL, body)
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

func (r *SyncRunner) process(ctx context.Context, sync *v1alpha1.Sync) {
	srcReq, err := r.buildRequest(&sync.Spec.Source, nil)
	if err != nil {
		r.handleProcessError(ctx, sync, err)
		return
	}

	srcResp, err := r.httpClient.Do(srcReq)
	if err != nil {
		r.handleProcessError(ctx, sync, err)
		return
	}

	defer srcResp.Body.Close()

	if sync.Spec.Source.Response.StatusCode != 0 {
		if srcResp.StatusCode != sync.Spec.Source.Response.StatusCode {
			err := fmt.Errorf("unexpected status code from source: got %d, want %d",
				srcResp.StatusCode, sync.Spec.Source.Response.StatusCode)
			r.handleProcessError(ctx, sync, err)
			return
		}
	} else {
		if srcResp.StatusCode >= http.StatusMultipleChoices {
			err := fmt.Errorf("source returned error status code: %d", srcResp.StatusCode)
			r.handleProcessError(ctx, sync, err)
			return
		}
	}

	if srcResp.ContentLength != sync.Spec.Total {
		err := fmt.Errorf("content length mismatch: got %d, want %d", srcResp.ContentLength, sync.Spec.Total)
		r.handleProcessError(ctx, sync, err)
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

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		_, err := io.Copy(swmr, srcResp.Body)
		if err != nil {
			return err
		}

		return swmr.Close()
	})

	sync.Status.Etags = make([]string, len(sync.Spec.Destination))

	for i, dest := range sync.Spec.Destination {
		dest := dest
		i := i
		g.Go(func() error {
			destReq, err := r.buildRequest(&dest, swmr.NewReader())
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
			sync.Status.Etags[i] = etag
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		r.handleProcessError(ctx, sync, err)
		return
	}
	if sync.Spec.Sha256PartialPreviousName == "" {
		sync.Status.Progress = sync.Spec.Total
		sync.Status.Phase = v1alpha1.SyncPhaseSucceeded
		_, err := r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
		if err != nil {
			r.handleProcessError(ctx, sync, err)
			return
		}
		return
	}

	if sync.Spec.Sha256PartialPreviousName == "-" {
		err := r.processSha256(ctx, sync, swmr, nil)
		if err != nil {
			r.handleProcessError(ctx, sync, err)
			return
		}
		return
	}

	psync, err := r.syncInformer.Lister().Get(sync.Spec.Sha256PartialPreviousName)
	if err != nil {
		r.handleProcessError(ctx, sync, err)
		return
	}

	if psync.Status.Phase == v1alpha1.SyncPhaseSucceeded {
		if len(psync.Status.Sha256Partial) == 0 {
			err := fmt.Errorf("partial sync %q has no sha256 partial data", sync.Spec.Sha256PartialPreviousName)
			if err != nil {
				r.handleProcessError(ctx, sync, err)
			}
			return
		}

		err := r.processSha256(ctx, sync, swmr, psync.Status.Sha256Partial)
		if err != nil {
			r.handleProcessError(ctx, sync, err)
			return
		}
		return
	}

	sync.Status.Progress = sync.Spec.Total
	sync, err = r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
	if err != nil {
		r.handleProcessError(ctx, sync, err)
		return
	}

	ocleanup := cleanup
	cleanup = func() {}

	go func() {
		defer ocleanup()

		for {
			// TODO: Optimize to use push notifications in the future
			time.Sleep(time.Second)

			psync, err := r.syncInformer.Lister().Get(sync.Spec.Sha256PartialPreviousName)
			if err != nil {
				if err != nil {
					r.handleProcessError(ctx, sync, err)
				}
				return
			}

			if psync.Status.Phase != v1alpha1.SyncPhaseSucceeded {
				continue
			}

			if len(psync.Status.Sha256Partial) == 0 {
				err := fmt.Errorf("partial sync %q has no sha256 partial data", sync.Spec.Sha256PartialPreviousName)
				if err != nil {
					r.handleProcessError(ctx, sync, err)
				}
				return
			}

			err = r.processSha256(ctx, sync, swmr, psync.Status.Sha256Partial)
			if err != nil {
				r.handleProcessError(ctx, sync, err)
			}
			return
		}
	}()
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

	sync.Status.Progress = sync.Spec.Total
	sync.Status.Phase = v1alpha1.SyncPhaseSucceeded
	_, err := r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
	return err
}

func (r *SyncRunner) getPending(ctx context.Context) (*v1alpha1.Sync, error) {
	syncs, err := r.getPendingList()
	if err != nil {
		return nil, err
	}

	for _, sync := range syncs {
		sync := sync.DeepCopy()
		sync.Spec.HandlerName = r.handlerName
		sync.Status.Phase = v1alpha1.SyncPhaseRunning

		sync, err := r.client.TaskV1alpha1().Syncs().Update(ctx, sync, metav1.UpdateOptions{})
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
		if pendingSyncs[i].Spec.Weight != pendingSyncs[j].Spec.Weight {
			return pendingSyncs[i].Spec.Weight > pendingSyncs[j].Spec.Weight
		}
		return pendingSyncs[i].CreationTimestamp.Before(&pendingSyncs[j].CreationTimestamp)
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
