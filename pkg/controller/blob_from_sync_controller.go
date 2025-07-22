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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	s3           map[string]*sss.SSS
	client       versioned.Interface
	blobInformer informers.BlobInformer
	syncInformer informers.SyncInformer
	workqueue    workqueue.TypedDelayingInterface[string]
}

func NewBlobFromSyncController(
	handlerName string,
	s3 map[string]*sss.SSS,
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

			blobName := newSync.Annotations[BlobNameAnnotationKey]
			if blobName == "" {
				return
			}
			c.workqueue.Add(blobName)
		},
		DeleteFunc: func(obj interface{}) {
			sync := obj.(*v1alpha1.Sync)

			blobName := sync.Annotations[BlobNameAnnotationKey]
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
	if blob.Spec.ChunksNumber == 1 {
		sync, err := c.syncInformer.Lister().Get(blob.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get sync: %w", err)
		}

		switch sync.Status.Phase {
		case v1alpha1.SyncPhaseSucceeded:
			blob.Status.PendingChunks = 0
			blob.Status.RunningChunks = 0
			blob.Status.SucceededChunks = 1
			blob.Status.FailedChunks = 0
			err := c.verifySha256(ctx, blob)
			if err != nil {
				blob.Status.Phase = v1alpha1.BlobPhaseFailed
				blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, v1alpha1.Condition{
					Type:    "Sha256Verification",
					Message: err.Error(),
				})
			} else {
				blob.Status.Phase = v1alpha1.BlobPhaseSucceeded
				blob.Status.Progress = sync.Status.Progress
			}
		case v1alpha1.SyncPhaseFailed:
			blob.Status.PendingChunks = 0
			blob.Status.RunningChunks = 0
			blob.Status.SucceededChunks = 0
			blob.Status.FailedChunks = 1
			if _, ok := v1alpha1.GetCondition(sync.Status.Conditions, v1alpha1.ConditionTypeRetryable); ok && sync.Status.RetryCount < sync.Spec.RetryCount {
				blob.Status.Phase = v1alpha1.BlobPhaseRunning
				blob.Status.Progress = sync.Status.Progress
			} else {
				blob.Status.RetryCount = sync.Status.RetryCount
				blob.Status.Phase = v1alpha1.BlobPhaseFailed
				for _, cond := range sync.Status.Conditions {
					if cond.Type == v1alpha1.ConditionTypeRetryable {
						continue
					}
					blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, cond)
				}
			}
		case v1alpha1.SyncPhaseRunning, v1alpha1.SyncPhaseUnknown:
			blob.Status.PendingChunks = 0
			blob.Status.RunningChunks = 1
			blob.Status.SucceededChunks = 0
			blob.Status.FailedChunks = 0
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
			blob.Status.Progress = sync.Status.Progress
		case v1alpha1.SyncPhasePending:
			blob.Status.PendingChunks = 1
			blob.Status.RunningChunks = 0
			blob.Status.SucceededChunks = 0
			blob.Status.FailedChunks = 0
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
			blob.Status.Progress = sync.Status.Progress
		}
		return nil
	}

	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		return fmt.Errorf("failed to list syncs: %w", err)
	}

	if len(blob.Status.UploadEtags) == 0 {
		blob.Status.UploadEtags = make([]v1alpha1.UploadEtag, blob.Spec.ChunksNumber)
	}

	var succeededCount, failedCount, pendingCount, runningCount, retryCount int64
	var progress int64
	for _, sync := range syncs {
		switch sync.Status.Phase {
		case v1alpha1.SyncPhaseSucceeded:

			blob.Status.UploadEtags[sync.Spec.ChunkIndex-1] = v1alpha1.UploadEtag{
				Size:  sync.Spec.Total,
				Etags: sync.Status.Etags,
			}
			continue
		case v1alpha1.SyncPhaseFailed:
			failedCount++
		case v1alpha1.SyncPhasePending:
			pendingCount++
		case v1alpha1.SyncPhaseRunning:
			runningCount++
		}

		retryCount += sync.Status.RetryCount
		progress += sync.Status.Progress
	}

	for _, e := range blob.Status.UploadEtags {
		if len(e.Etags) != 0 {
			succeededCount++
			progress += e.Size
		}
	}

	blob.Status.Progress = progress
	blob.Status.PendingChunks = pendingCount
	blob.Status.RunningChunks = runningCount
	blob.Status.SucceededChunks = succeededCount
	blob.Status.FailedChunks = failedCount

	if failedCount != 0 {
		if retryCount >= blob.Spec.RetryCount {
			blob.Status.RetryCount = blob.Spec.RetryCount
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			for _, sync := range syncs {
				if sync.Status.Phase == v1alpha1.SyncPhaseFailed {
					blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, sync.Status.Conditions...)
				}
			}
		} else {
			hasNonRetryableFailure := false
			for _, sync := range syncs {
				if sync.Status.Phase == v1alpha1.SyncPhaseFailed {
					if _, ok := v1alpha1.GetCondition(sync.Status.Conditions, v1alpha1.ConditionTypeRetryable); !ok {
						hasNonRetryableFailure = true
						break
					}
				}
			}
			if hasNonRetryableFailure {
				blob.Status.RetryCount = blob.Spec.RetryCount
				blob.Status.Phase = v1alpha1.BlobPhaseFailed
				for _, sync := range syncs {
					if sync.Status.Phase == v1alpha1.SyncPhaseFailed {
						for _, cond := range sync.Status.Conditions {
							if cond.Type == v1alpha1.ConditionTypeRetryable {
								continue
							}
							blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, cond)
						}
					}
				}
			} else {
				blob.Status.Phase = v1alpha1.BlobPhaseRunning
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

		var totalSize int64
		for _, etag := range blob.Status.UploadEtags {
			totalSize += etag.Size
		}
		if totalSize != blob.Spec.Total {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
				Type:    "SizeMismatch",
				Message: fmt.Sprintf("total size of uploaded parts (%d) does not match expected total (%d)", totalSize, blob.Spec.Total),
			})
			return nil
		}

		g, _ := errgroup.WithContext(ctx)

		for i, dst := range blob.Spec.Destination {
			uploadID := blob.Status.UploadIDs[i]
			dst := dst
			parts := make([]*s3.Part, 0, len(blob.Status.UploadEtags))
			for j, s := range blob.Status.UploadEtags {
				partNumber := int64(j + 1)
				parts = append(parts, &s3.Part{
					ETag:       &s.Etags[i],
					PartNumber: &partNumber,
					Size:       &s.Size,
				})
			}

			s3 := c.s3[dst.Name]
			if s3 == nil {
				return fmt.Errorf("s3 client for destination %q not found", dst.Name)
			}

			mp := s3.GetMultipartWithUploadID(dst.Path, uploadID)
			mp.SetParts(parts)

			g.Go(func() error {
				return mp.Commit(ctx)
			})
		}
		err = g.Wait()
		if err != nil {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
				Type:    "MultipartCommit",
				Message: err.Error(),
			})
			return nil
		}

		err := c.verifySha256(ctx, blob)
		if err != nil {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, v1alpha1.Condition{
				Type:    "Sha256Verification",
				Message: err.Error(),
			})
		} else {
			blob.Status.UploadIDs = blob.Status.UploadIDs[:0]
			blob.Status.UploadEtags = blob.Status.UploadEtags[:0]
			blob.Status.Phase = v1alpha1.BlobPhaseSucceeded
		}
	}
	return nil
}

func (c *BlobFromSyncController) verifySha256(ctx context.Context, blob *v1alpha1.Blob) error {
	if blob.Spec.ContentSha256 == "" {
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, dst := range blob.Spec.Destination {
		if !dst.VerifySha256 {
			continue
		}
		dst := dst

		s3 := c.s3[dst.Name]
		if s3 == nil {
			return fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		g.Go(func() error {
			return verifyDestinationSha256(ctx, s3, &dst, blob.Spec.ContentSha256)
		})
	}
	return g.Wait()
}

func verifyDestinationSha256(ctx context.Context, s3 *sss.SSS, dst *v1alpha1.BlobDestination, contentSha256 string) error {
	reader, err := s3.Reader(ctx, dst.Path)
	if err != nil {
		return fmt.Errorf("failed to open reader for destination %q: %w", dst.Path, err)
	}
	defer reader.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, reader); err != nil {
		return fmt.Errorf("failed to read content for destination %q: %w", dst.Path, err)
	}

	calculatedHash := hex.EncodeToString(hasher.Sum(nil))
	if calculatedHash != contentSha256 {
		return fmt.Errorf("sha256 mismatch for destination %q: expected %s, got %s", dst.Path, contentSha256, calculatedHash)
	}
	return nil
}
