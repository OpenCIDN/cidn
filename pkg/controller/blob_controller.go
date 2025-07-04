package controller

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wzshiming/sss"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
)

type BlobController struct {
	handlerName string

	s3 *sss.SSS

	expires time.Duration

	httpClient *http.Client

	client                versioned.Interface
	blobInformer          informers.BlobInformer
	syncInformer          informers.SyncInformer
	sharedInformerFactory externalversions.SharedInformerFactory
	workqueue             workqueue.TypedRateLimitingInterface[string]
}

func NewBlobController(
	handlerName string,
	s3 *sss.SSS,
	client versioned.Interface,
) *BlobController {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)
	return &BlobController{
		handlerName:           handlerName,
		s3:                    s3,
		expires:               24 * time.Hour,
		httpClient:            http.DefaultClient,
		blobInformer:          sharedInformerFactory.Task().V1alpha1().Blobs(),
		syncInformer:          sharedInformerFactory.Task().V1alpha1().Syncs(),
		sharedInformerFactory: sharedInformerFactory,
		client:                client,
		workqueue:             workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
}

func (c *BlobController) Start(ctx context.Context) error {
	c.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			blob := obj.(*v1alpha1.Blob)
			key, err := cache.MetaNamespaceKeyFunc(blob)
			if err != nil {
				klog.Errorf("couldn't get key for object %+v: %v", blob, err)
				return
			}
			c.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			blob := newObj.(*v1alpha1.Blob)
			key, err := cache.MetaNamespaceKeyFunc(blob)
			if err != nil {
				klog.Errorf("couldn't get key for object %+v: %v", blob, err)
				return
			}
			c.workqueue.Add(key)
		},
		DeleteFunc: func(obj interface{}) {
			blob, ok := obj.(*v1alpha1.Blob)
			if !ok {
				return
			}

			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(blob)
			if err != nil {
				klog.Errorf("couldn't get key for object %+v: %v", blob, err)
				return
			}
			c.workqueue.Forget(key)

			err = c.client.TaskV1alpha1().Syncs().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
				LabelSelector: labels.Set{BlobNameLabel: blob.Name}.String(),
			})
			if err != nil {
				klog.Errorf("failed to delete syncs for blob %s: %v", blob.Name, err)
			}
		},
	})

	c.syncInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newSync := newObj.(*v1alpha1.Sync)
			oldSync := oldObj.(*v1alpha1.Sync)

			if reflect.DeepEqual(newSync.Status, oldSync.Status) {
				return
			}

			blobName := newSync.Labels[BlobNameLabel]
			if blobName == "" {
				return
			}

			key, err := cache.MetaNamespaceKeyFunc(&metav1.ObjectMeta{Name: blobName})
			if err != nil {
				klog.Errorf("couldn't get key for blob %s: %v", blobName, err)
				return
			}
			c.workqueue.Add(key)
		},
	})

	go c.runWorker(ctx)
	c.sharedInformerFactory.Start(ctx.Done())
	return nil
}

func (c *BlobController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BlobController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.syncHandler(ctx, key)
	if err != nil {
		c.workqueue.AddRateLimited(key)
		klog.Errorf("error syncing '%s': %v, requeuing", key, err)
		return true
	}

	c.workqueue.Forget(key)
	return true
}

func (c *BlobController) syncHandler(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("invalid resource key: %s", key)
		return nil
	}

	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Handle blob deletion
			err := c.client.TaskV1alpha1().Syncs().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
				LabelSelector: labels.Set{BlobNameLabel: name}.String(),
			})
			if err != nil {
				return fmt.Errorf("failed to delete syncs for blob %s: %v", name, err)
			}
			return nil
		}
		return err
	}

	if blob.Spec.Total == 0 {
		return nil
	}
	if blob.Status.Phase != v1alpha1.BlobPhasePending && blob.Status.Phase != v1alpha1.BlobPhaseRunning {
		return nil
	}

	if blob.Spec.HandlerName == "" {
		blob.Spec.HandlerName = c.handlerName
		newBlob, err := c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update blob %s: %v", blob.Name, err)
		}
		blob = newBlob
	}

	if blob.Spec.HandlerName != c.handlerName {
		return nil
	}

	oldBlob := blob.DeepCopy()

	defer func() {
		if !reflect.DeepEqual(oldBlob.Status, blob.Status) {
			_, err := c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("failed to update blob status: %v", err)
			}
		}
	}()

	if blob.Spec.ChunkSize != 0 && blob.Spec.Total > blob.Spec.ChunkSize {
		klog.Infof("Creating multiple syncs for blob %s with total size %d and chunk size %d", blob.Name, blob.Spec.Total, blob.Spec.ChunkSize)
		err := c.createSyncs(ctx, blob)
		if err != nil {
			return fmt.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
		}
	} else {
		klog.Infof("Creating single sync for blob %s with total size %d", blob.Name, blob.Spec.Total)
		err := c.createOneSync(ctx, blob)
		if err != nil {
			return fmt.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
		}
	}

	return c.updateBlobStatusFromSyncs(ctx, blob)
}

func (c *BlobController) createOneSync(ctx context.Context, blob *v1alpha1.Blob) error {
	existingSync, err := c.syncInformer.Lister().Get(blob.Name)
	if err == nil && existingSync != nil {
		// Sync already exists, no need to create a new one
		return nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing sync: %v", err)
	}
	sync := &v1alpha1.Sync{
		ObjectMeta: metav1.ObjectMeta{
			Name: blob.Name,
			Labels: map[string]string{
				BlobNameLabel: blob.Name,
			},
		},
		Spec: v1alpha1.SyncSpec{
			Total:  blob.Spec.Total,
			Weight: blob.Spec.Weight,
			Sha256: blob.Spec.Sha256,
		},
		Status: v1alpha1.SyncStatus{
			Phase: v1alpha1.SyncPhasePending,
		},
	}

	if blob.Spec.Sha256 != "" {
		sync.Spec.Sha256PartialPreviousName = "-"
	}

	sync.Spec.Source = v1alpha1.SyncHTTP{
		Request: v1alpha1.SyncHTTPRequest{
			Method: http.MethodGet,
			URL:    blob.Spec.Source,
		},
	}
	for _, dst := range blob.Spec.Destination {
		d, err := c.s3.SignPut(dst, c.expires)
		if err != nil {
			return err
		}

		sync.Spec.Destination = append(sync.Spec.Destination, v1alpha1.SyncHTTP{
			Request: v1alpha1.SyncHTTPRequest{
				Method: http.MethodPut,
				URL:    d,
				Headers: map[string]string{
					"Content-Length": fmt.Sprintf("%d", blob.Spec.Total),
				},
			},
			Response: v1alpha1.SyncHTTPResponse{
				StatusCode: http.StatusOK,
			},
		})
	}

	_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// getChunkCount returns the number of chunks needed for the blob based on total size and chunk size
func getChunkCount(blob *v1alpha1.Blob) int64 {
	chunkSize := blob.Spec.ChunkSize
	if chunkSize == 0 {
		return 1
	}

	total := blob.Spec.Total
	if chunkSize >= total {
		return 1
	}

	count := total / chunkSize
	if total%chunkSize != 0 {
		count++
	}
	return count
}

const BlobNameLabel = v1alpha1.GroupName + "/blob-name"

func (c *BlobController) createSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
	chunks := getChunkCount(blob)

	uploadIDs := blob.Status.UploadIDs
	if len(uploadIDs) == 0 {
		for _, dst := range blob.Spec.Destination {
			mp, err := c.s3.NewMultipart(ctx, dst)
			if err != nil {
				return err
			}
			uploadIDs = append(uploadIDs, mp.UploadID())
		}

		blob.Status.UploadIDs = uploadIDs
	}

	// Get current syncs from informer cache
	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobNameLabel: blob.Name,
	}))
	if err != nil {
		return err
	}

	// Count pending and running syncs
	pendingCount := 0
	runningCount := 0
	failedCount := 0
	for _, sync := range syncs {
		switch sync.Status.Phase {
		case v1alpha1.SyncPhasePending:
			pendingCount++
		case v1alpha1.SyncPhaseRunning:
			runningCount++
		case v1alpha1.SyncPhaseFailed:
			failedCount++
		}
	}

	// Only create new syncs if we have room
	if failedCount != 0 || pendingCount >= int(blob.Spec.PendingSize) || runningCount >= int(blob.Spec.RunningSize) {
		return nil
	}

	// Calculate how many new syncs we can create
	toCreate := int(blob.Spec.PendingSize) - pendingCount
	if toCreate <= 0 {
		return nil
	}

	created := 0

	apiVersion := v1alpha1.GroupVersion.String()

	p0 := int(math.Log10(float64(chunks + 1)))
	lastName := "-"
	for i := int64(0); i < int64(chunks) && created < toCreate; i++ {

		start := i * blob.Spec.ChunkSize
		end := start + blob.Spec.ChunkSize
		if end > blob.Spec.Total {
			end = blob.Spec.Total
		}

		num := i + 1
		name := fmt.Sprintf("%s-%0*d-%d-%d", blob.Name, p0, num, start, end)

		_, err := c.syncInformer.Lister().Get(name)
		if err == nil {
			lastName = name
			continue
		}
		if !apierrors.IsNotFound(err) {
			return err
		}

		sync := &v1alpha1.Sync{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					BlobNameLabel: blob.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: apiVersion,
						Kind:       v1alpha1.BlobKind,
						Name:       blob.Name,
						UID:        blob.UID,
					},
				},
			},
			Spec: v1alpha1.SyncSpec{
				Total:      end - start,
				Weight:     blob.Spec.Weight,
				PartNumber: num,
			},
			Status: v1alpha1.SyncStatus{
				Phase: v1alpha1.SyncPhasePending,
			},
		}

		if blob.Spec.Sha256 != "" {
			sync.Spec.Sha256PartialPreviousName = lastName
		}

		if num == chunks && blob.Spec.Sha256 != "" {
			sync.Spec.Sha256 = blob.Spec.Sha256
		}

		sync.Spec.Source = v1alpha1.SyncHTTP{
			Request: v1alpha1.SyncHTTPRequest{
				Method: http.MethodGet,
				URL:    blob.Spec.Source,
				Headers: map[string]string{
					"Range": fmt.Sprintf("bytes=%d-%d", start, end-1),
				},
			},
		}

		for j, dst := range blob.Spec.Destination {
			mp := c.s3.GetMultipartWithUploadID(dst, blob.Status.UploadIDs[j])

			partURL, err := mp.SignUploadPart(num, c.expires)
			if err != nil {
				return err
			}

			sync.Spec.Destination = append(sync.Spec.Destination, v1alpha1.SyncHTTP{
				Request: v1alpha1.SyncHTTPRequest{
					Method: http.MethodPut,
					URL:    partURL,
					Headers: map[string]string{
						"Content-Length": fmt.Sprintf("%d", end-start),
					},
				},
				Response: v1alpha1.SyncHTTPResponse{
					StatusCode: http.StatusOK,
				},
			})
		}

		_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		created++
		lastName = name
	}

	return nil
}

func (c *BlobController) updateBlobStatusFromSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobNameLabel: blob.Name,
	}))
	if err != nil {
		return fmt.Errorf("failed to list syncs: %w", err)
	}

	chunks := getChunkCount(blob)

	var succeeded, failed, pending int64
	var progress int64
	for _, sync := range syncs {
		switch sync.Status.Phase {
		case v1alpha1.SyncPhaseSucceeded:
			succeeded++
		case v1alpha1.SyncPhaseFailed:
			failed++
		case v1alpha1.SyncPhasePending:
			pending++
		}
		progress += sync.Status.Progress
	}

	blob.Status.Progress = progress

	if failed != 0 {
		blob.Status.Phase = v1alpha1.BlobPhaseFailed
		for _, sync := range syncs {
			if sync.Status.Phase == v1alpha1.SyncPhaseFailed {
				for _, condition := range sync.Status.Conditions {
					if condition.Status == v1alpha1.ConditionTrue {
						blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
							Type:               condition.Type,
							Status:             condition.Status,
							Reason:             condition.Reason,
							Message:            condition.Message,
							LastTransitionTime: condition.LastTransitionTime,
						})
					}
				}
			}
		}
		return fmt.Errorf("one or more syncs failed")
	}

	if chunks != succeeded {
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
		return nil
	}

	if len(blob.Status.UploadIDs) != 0 && blob.Status.Phase == v1alpha1.BlobPhaseRunning {
		g, ctx := errgroup.WithContext(ctx)
		for i, dst := range blob.Spec.Destination {
			uploadID := blob.Status.UploadIDs[i]
			dst := dst
			g.Go(func() error {
				var parts []*s3.Part
				for _, s := range syncs {
					parts = append(parts, &s3.Part{
						ETag:       &s.Status.Etags[i],
						PartNumber: &s.Spec.PartNumber,
						Size:       &s.Spec.Total,
					})
				}

				mp := c.s3.GetMultipartWithUploadID(dst, uploadID)
				mp.SetParts(parts)
				return mp.Commit(ctx)
			})
		}
		err = g.Wait()
		if err != nil {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
				Type:               "MultipartCommit",
				Status:             v1alpha1.ConditionTrue,
				Reason:             "MultipartCommitFailed",
				Message:            err.Error(),
				LastTransitionTime: metav1.Now(),
			})
			return nil
		}
		blob.Status.UploadIDs = nil
	}
	blob.Status.Phase = v1alpha1.BlobPhaseSucceeded

	return nil
}
