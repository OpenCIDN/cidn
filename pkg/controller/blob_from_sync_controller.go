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

package controller

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wzshiming/sss"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type BlobFromSyncController struct {
	handlerName  string
	s3           *sss.SSS
	client       versioned.Interface
	blobInformer informers.BlobInformer
	syncInformer informers.SyncInformer
	workqueue    workqueue.TypedDelayingInterface[string]
}

func NewBlobFromSyncController(
	handlerName string,
	s3 *sss.SSS,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobFromSyncController {
	c := &BlobFromSyncController{
		handlerName:  handlerName,
		s3:           s3,
		blobInformer: sharedInformerFactory.Task().V1alpha1().Blobs(),
		syncInformer: sharedInformerFactory.Task().V1alpha1().Syncs(),
		client:       client,
		workqueue:    workqueue.NewTypedDelayingQueue[string](),
	}

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
			c.workqueue.Add(blobName)
		},
		DeleteFunc: func(obj interface{}) {
			sync := obj.(*v1alpha1.Sync)

			blobName := sync.Labels[BlobNameLabel]
			if blobName == "" {
				return
			}
			c.workqueue.Add(blobName)
		},
	})

	return c
}

func (c *BlobFromSyncController) Start(ctx context.Context) error {

	go c.runWorker(ctx)
	return nil
}

func (c *BlobFromSyncController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BlobFromSyncController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.syncHandler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error blob syncing '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *BlobFromSyncController) syncHandler(ctx context.Context, name string) error {
	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if blob.Spec.HandlerName != c.handlerName {
		return nil
	}

	if blob.Status.Phase == v1alpha1.BlobPhaseSucceeded {
		return nil
	}

	err = c.updateBlobStatusFromSyncs(ctx, blob)
	if err != nil {
		return fmt.Errorf("failed to update blob status for blob %s: %v", blob.Name, err)
	}

	_, err = c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update blob status: %v", err)
	}

	return nil
}

func (c *BlobFromSyncController) updateBlobStatusFromSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobNameLabel: blob.Name,
		BlobUIDLabel:  string(blob.UID),
	}))
	if err != nil {
		return fmt.Errorf("failed to list syncs: %w", err)
	}

	if blob.Spec.ChunksNumber == 1 {
		if len(syncs) == 0 {
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
			return nil
		}
		sync := syncs[0]
		switch sync.Status.Phase {
		case v1alpha1.SyncPhaseSucceeded:
			blob.Status.Phase = v1alpha1.BlobPhaseSucceeded
			blob.Status.Progress = sync.Status.Progress
		case v1alpha1.SyncPhaseFailed:
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
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
		case v1alpha1.SyncPhaseRunning, v1alpha1.SyncPhaseUnknown, v1alpha1.SyncPhasePending:
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
			blob.Status.Progress = sync.Status.Progress
		}
		return nil
	}

	var succeededCount, failedCount, pendingCount, runningCount int64
	var progress int64
	for _, sync := range syncs {
		switch sync.Status.Phase {
		case v1alpha1.SyncPhaseSucceeded:
			succeededCount++
		case v1alpha1.SyncPhaseFailed:
			failedCount++
		case v1alpha1.SyncPhasePending:
			pendingCount++
		case v1alpha1.SyncPhaseRunning:
			runningCount++
		}
		progress += sync.Status.Progress
	}

	blob.Status.Progress = progress
	blob.Status.PendingChunks = pendingCount
	blob.Status.RunningChunks = runningCount
	blob.Status.SucceededChunks = succeededCount

	if failedCount != 0 {
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
		return nil
	}

	if blob.Spec.ChunksNumber != succeededCount {
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
		return nil
	}

	if len(blob.Status.UploadIDs) != 0 &&
		blob.Status.Phase == v1alpha1.BlobPhaseRunning &&
		blob.Status.Progress == blob.Spec.Total {
		g, _ := errgroup.WithContext(ctx)

		for i, dst := range blob.Spec.Destination {
			uploadID := blob.Status.UploadIDs[i]
			dst := dst
			g.Go(func() error {
				var parts []*s3.Part
				for _, s := range syncs {
					parts = append(parts, &s3.Part{
						ETag:       &s.Status.Etags[i],
						PartNumber: &s.Spec.ChunkIndex,
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
	}

	blob.Status.Phase = v1alpha1.BlobPhaseSucceeded

	return nil
}
