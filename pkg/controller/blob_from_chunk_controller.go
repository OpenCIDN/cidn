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
	"strconv"
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

type BlobFromChunkController struct {
	handlerName       string
	s3                map[string]*sss.SSS
	client            versioned.Interface
	blobInformer      informers.BlobInformer
	chunkInformer     informers.ChunkInformer
	multipartInformer informers.MultipartInformer
	workqueue         workqueue.TypedDelayingInterface[string]
}

func NewBlobFromChunkController(
	handlerName string,
	s3 map[string]*sss.SSS,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobFromChunkController {
	c := &BlobFromChunkController{
		handlerName:       handlerName,
		s3:                s3,
		blobInformer:      sharedInformerFactory.Task().V1alpha1().Blobs(),
		chunkInformer:     sharedInformerFactory.Task().V1alpha1().Chunks(),
		multipartInformer: sharedInformerFactory.Task().V1alpha1().Multiparts(),
		client:            client,
		workqueue:         workqueue.NewTypedDelayingQueue[string](),
	}

	c.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newChunk := newObj.(*v1alpha1.Chunk)
			oldChunk := oldObj.(*v1alpha1.Chunk)

			if reflect.DeepEqual(newChunk.Status, oldChunk.Status) {
				return
			}

			blobName := newChunk.Annotations[BlobNameAnnotationKey]
			if blobName == "" {
				return
			}
			c.workqueue.Add(blobName)
		},
		DeleteFunc: func(obj interface{}) {
			chunk := obj.(*v1alpha1.Chunk)

			blobName := chunk.Annotations[BlobNameAnnotationKey]
			if blobName == "" {
				return
			}
			c.workqueue.Add(blobName)
		},
	})

	return c
}

func (c *BlobFromChunkController) Start(ctx context.Context) error {

	go c.runWorker(ctx)
	return nil
}

func (c *BlobFromChunkController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BlobFromChunkController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.chunkHandler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error blob chunking '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *BlobFromChunkController) chunkHandler(ctx context.Context, name string) error {
	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if blob.Status.HandlerName != c.handlerName {
		return nil
	}

	if blob.Status.Phase == v1alpha1.BlobPhaseSucceeded {
		return nil
	}

	if blob.Status.Total == 0 {
		updateBlob := blob.DeepCopy()
		err = c.fromHeadChunk(ctx, updateBlob)
		if err != nil {
			return fmt.Errorf("failed to update blob status for blob %s: %w", updateBlob.Name, err)
		}

		if !reflect.DeepEqual(updateBlob.Status, blob.Status) {
			_, err = c.client.TaskV1alpha1().Blobs().UpdateStatus(ctx, updateBlob, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to update blob status total: %w", err)
			}
		}

		return nil
	}

	if blob.Spec.ChunksNumber == 0 {
		return nil
	}

	updateBlob := blob.DeepCopy()

	if blob.Spec.ChunksNumber != 1 && blob.Status.AcceptRanges {
		err = c.fromChunks(ctx, updateBlob)
		if err != nil {
			return fmt.Errorf("failed to update blob status for blob %s: %w", updateBlob.Name, err)
		}
	} else {
		err = c.fromOneChunk(ctx, updateBlob)
		if err != nil {
			return fmt.Errorf("failed to update blob status for blob %s: %w", updateBlob.Name, err)
		}
	}
	if !reflect.DeepEqual(updateBlob.Status, blob.Status) {
		_, err = c.client.TaskV1alpha1().Blobs().UpdateStatus(ctx, updateBlob, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update blob status: %w", err)
		}
	}

	return nil
}

func (c *BlobFromChunkController) fromHeadChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	chunkName := buildHeadChunkName(blob.Name, 0)
	chunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get chunk: %w", err)
	}

	switch chunk.Status.Phase {
	case v1alpha1.ChunkPhaseSucceeded:
		total, err := strconv.ParseInt(chunk.Status.SourceResponse.Headers["content-length"], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse content-length: %w", err)
		}

		blob.Status.Total = total
		blob.Status.AcceptRanges = chunk.Status.SourceResponse.Headers["accept-ranges"] == "bytes"

	case v1alpha1.ChunkPhaseFailed:
		blob.Status.Retry = chunk.Status.Retry
		if _, ok := v1alpha1.GetCondition(chunk.Status.Conditions, v1alpha1.ConditionTypeRetryable); ok && chunk.Status.Retry < chunk.Spec.MaximumRetry {
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
		} else {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			for _, cond := range chunk.Status.Conditions {
				if cond.Type == v1alpha1.ConditionTypeRetryable {
					continue
				}
				blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, cond)
			}
		}
	}
	return nil
}

func (c *BlobFromChunkController) fromOneChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	chunkName := buildFullChunkName(blob.Name)
	chunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get chunk: %w", err)
	}

	switch chunk.Status.Phase {
	case v1alpha1.ChunkPhaseSucceeded:
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
			blob.Status.Progress = chunk.Status.Progress
		}
	case v1alpha1.ChunkPhaseFailed:
		blob.Status.PendingChunks = 0
		blob.Status.RunningChunks = 0
		blob.Status.SucceededChunks = 0
		blob.Status.FailedChunks = 1
		blob.Status.Retry = chunk.Status.Retry
		if _, ok := v1alpha1.GetCondition(chunk.Status.Conditions, v1alpha1.ConditionTypeRetryable); ok && chunk.Status.Retry < chunk.Spec.MaximumRetry {
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
			blob.Status.Progress = chunk.Status.Progress
		} else {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			for _, cond := range chunk.Status.Conditions {
				if cond.Type == v1alpha1.ConditionTypeRetryable {
					continue
				}
				blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, cond)
			}
		}
	case v1alpha1.ChunkPhaseRunning, v1alpha1.ChunkPhaseUnknown:
		blob.Status.PendingChunks = 0
		blob.Status.RunningChunks = 1
		blob.Status.SucceededChunks = 0
		blob.Status.FailedChunks = 0
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
		blob.Status.Progress = chunk.Status.Progress
	case v1alpha1.ChunkPhasePending:
		blob.Status.PendingChunks = 1
		blob.Status.RunningChunks = 0
		blob.Status.SucceededChunks = 0
		blob.Status.FailedChunks = 0
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
		blob.Status.Progress = chunk.Status.Progress
	}
	return nil
}

func (c *BlobFromChunkController) fromChunks(ctx context.Context, blob *v1alpha1.Blob) error {
	chunks, err := c.chunkInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	mp, err := c.multipartInformer.Lister().Get(blob.Name)
	if err != nil {
		return fmt.Errorf("failed to list multiparts: %w", err)
	}

	var updateEtag bool
	var succeededCount, failedCount, pendingCount, runningCount, retryCount int64
	var progress int64
	for _, chunk := range chunks {
		switch chunk.Status.Phase {
		case v1alpha1.ChunkPhaseSucceeded:
			if chunk.Spec.ChunkIndex != 0 {
				index := chunk.Spec.ChunkIndex - 1
				if mp.UploadEtags[index].Size == 0 {
					mp.UploadEtags[index] = v1alpha1.UploadEtags{
						Size:  chunk.Spec.Total,
						Etags: chunk.Status.Etags,
					}
					updateEtag = true
				}
			}
			continue

		case v1alpha1.ChunkPhaseFailed:
			failedCount++
		case v1alpha1.ChunkPhasePending:
			pendingCount++
		case v1alpha1.ChunkPhaseRunning:
			runningCount++
		}

		retryCount += chunk.Status.Retry
		progress += chunk.Status.Progress
	}

	if updateEtag {
		go func() {
			_, err = c.client.TaskV1alpha1().Multiparts().Update(ctx, mp, metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("Failed to update multipart %s: %v", mp.Name, err)
			}
		}()
	}

	for _, e := range mp.UploadEtags {
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
		if retryCount >= blob.Spec.MaximumRetry {
			blob.Status.Retry = blob.Spec.MaximumRetry
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			for _, chunk := range chunks {
				if chunk.Status.Phase == v1alpha1.ChunkPhaseFailed {
					blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, chunk.Status.Conditions...)
				}
			}
		} else {
			hasNonRetryableFailure := false
			for _, chunk := range chunks {
				if chunk.Status.Phase == v1alpha1.ChunkPhaseFailed {
					if _, ok := v1alpha1.GetCondition(chunk.Status.Conditions, v1alpha1.ConditionTypeRetryable); !ok {
						hasNonRetryableFailure = true
						break
					}
				}
			}
			if hasNonRetryableFailure {
				blob.Status.Retry = blob.Spec.MaximumRetry
				blob.Status.Phase = v1alpha1.BlobPhaseFailed
				for _, chunk := range chunks {
					if chunk.Status.Phase == v1alpha1.ChunkPhaseFailed {
						for _, cond := range chunk.Status.Conditions {
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

	if len(mp.UploadIDs) != 0 &&
		blob.Status.Phase == v1alpha1.BlobPhaseRunning &&
		blob.Status.Progress == blob.Status.Total {

		var totalSize int64
		for _, etag := range mp.UploadEtags {
			totalSize += etag.Size
		}
		if totalSize != blob.Status.Total {
			blob.Status.Phase = v1alpha1.BlobPhaseFailed
			blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
				Type:    "SizeMismatch",
				Message: fmt.Sprintf("total size of uploaded parts (%d) does not match expected total (%d)", totalSize, blob.Status.Total),
			})
			return nil
		}

		g, _ := errgroup.WithContext(ctx)

		for i, dst := range blob.Spec.Destination {
			uploadID := mp.UploadIDs[i]
			if uploadID == "" {
				continue
			}
			dst := dst
			parts := make([]*s3.Part, 0, len(mp.UploadEtags))
			for j, s := range mp.UploadEtags {
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
			blob.Status.Phase = v1alpha1.BlobPhaseSucceeded

			err = c.client.TaskV1alpha1().Multiparts().Delete(ctx, blob.Name, metav1.DeleteOptions{})
			if err != nil {
				klog.Errorf("failed to delete multipart %s: %v", blob.Name, err)
				return nil
			}
		}
	}
	return nil
}

func (c *BlobFromChunkController) verifySha256(ctx context.Context, blob *v1alpha1.Blob) error {
	if blob.Spec.ContentSha256 == "" {
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, dst := range blob.Spec.Destination {
		if !dst.ReverifySha256 {
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
