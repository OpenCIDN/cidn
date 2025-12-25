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
	"strconv"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
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
	concurrency       int
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
		concurrency:       5,
	}

	c.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			blob := obj.(*v1alpha1.Blob)
			c.workqueue.AddAfter(blob.Name, 10*time.Second)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			blob := newObj.(*v1alpha1.Blob)
			c.workqueue.AddAfter(blob.Name, 10*time.Second)
		},
	})

	c.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chunk := obj.(*v1alpha1.Chunk)
			blobName := chunk.Annotations[BlobNameAnnotationKey]
			if blobName == "" {
				return
			}
			c.workqueue.Add(blobName)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			chunk := newObj.(*v1alpha1.Chunk)
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
	for i := 0; i < c.concurrency; i++ {
		go c.runWorker(ctx)
	}
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

	c.handler(ctx, key)

	return true
}

func (c *BlobFromChunkController) handler(ctx context.Context, name string) {
	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to get blob '%s': %v", name, err)
			return
		}
		blob, err = c.client.TaskV1alpha1().Blobs().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to get blob '%s' from API server: %v", name, err)
			return
		}
	}

	if blob.Status.HandlerName != c.handlerName {
		return
	}

	if blob.Status.Phase == v1alpha1.BlobPhaseSucceeded || blob.Status.Phase == v1alpha1.BlobPhaseFailed {
		return
	}

	blob = blob.DeepCopy()

	if blob.Spec.ChunksNumber == 1 {
		err = c.fromOneChunk(ctx, blob)
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to update blob status for blob %s: %v", blob.Name, err)
			return
		}

		blobStatus := blob.Status
		_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Blobs(), blob, func(blob *v1alpha1.Blob) *v1alpha1.Blob {
			blob.Status = blobStatus
			return blob
		})
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to update blob status: %v", err)
			return
		}

		c.workqueue.AddAfter(name, 10*time.Second)
		return
	}

	if blob.Status.Total == 0 {
		err = c.fromHeadChunk(ctx, blob)
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to update blob status for blob %s: %v", blob.Name, err)
			return
		}

		blobStatus := blob.Status
		_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Blobs(), blob, func(blob *v1alpha1.Blob) *v1alpha1.Blob {
			blob.Status = blobStatus
			return blob
		})
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			return
		}

		c.workqueue.AddAfter(name, 10*time.Second)
		return
	}

	if blob.Spec.ChunksNumber == 0 {
		c.workqueue.AddAfter(name, 10*time.Second)
		return
	}

	if blob.Status.AcceptRanges {
		err = c.fromChunks(ctx, blob)
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to update blob status for blob %s: %v", blob.Name, err)
			return
		}

		blobStatus := blob.Status
		_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Blobs(), blob, func(blob *v1alpha1.Blob) *v1alpha1.Blob {
			blob.Status = blobStatus
			return blob
		})
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to update blob status: %v", err)
			return
		}

		c.workqueue.AddAfter(name, 10*time.Second)
		return
	}

	err = c.fromOneChunk(ctx, blob)
	if err != nil {
		c.workqueue.AddAfter(name, 5*time.Second)
		klog.Errorf("failed to update blob status for blob %s: %v", blob.Name, err)
		return
	}

	blobStatus := blob.Status
	_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Blobs(), blob, func(blob *v1alpha1.Blob) *v1alpha1.Blob {
		blob.Status = blobStatus
		return blob
	})
	if err != nil {
		c.workqueue.AddAfter(name, 5*time.Second)
		klog.Errorf("failed to update blob status: %v", err)
		return
	}

	c.workqueue.AddAfter(name, 10*time.Second)
}

func (c *BlobFromChunkController) fromHeadChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	chunkName := buildHeadChunkName(blob.Name, 0)
	chunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err != nil {
		return fmt.Errorf("failed to get chunk: %w", err)
	}

	switch chunk.Status.Phase {
	case v1alpha1.ChunkPhaseSucceeded:
		if chunk.Status.SourceResponse == nil {
			return fmt.Errorf("chunk %s succeeded but has no source response", chunkName)
		}

		total, _ := strconv.ParseInt(chunk.Status.SourceResponse.Headers["content-length"], 10, 64)
		if total <= 0 {
			blob.Status.Total = -1
		} else {
			blob.Status.Total = total
			// Use ForceAcceptRanges if set, otherwise check the server response
			if blob.Spec.ForceAcceptRanges {
				blob.Status.AcceptRanges = true
			} else {
				blob.Status.AcceptRanges = chunk.Status.SourceResponse.Headers["accept-ranges"] == "bytes"
			}
		}
		blob.Status.SourceResponseHeaders = chunk.Status.SourceResponse.Headers
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
	case v1alpha1.ChunkPhaseFailed:
		blob.Status.Retry = chunk.Status.Retry
		if chunk.Status.Retryable {
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
		} else {
			utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
			blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, chunk.Status.Conditions...)
		}
	}
	return nil
}

func (c *BlobFromChunkController) fromOneChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	chunkName := buildFullChunkName(blob.Name)
	chunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err != nil {
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
			utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
			blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, v1alpha1.Condition{
				Type:    "Sha256Verification",
				Message: err.Error(),
			})
		} else {
			utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseSucceeded)
			blob.Status.Progress = chunk.Status.Progress
		}
	case v1alpha1.ChunkPhaseFailed:
		blob.Status.PendingChunks = 0
		blob.Status.RunningChunks = 0
		blob.Status.SucceededChunks = 0
		blob.Status.FailedChunks = 1
		blob.Status.Retry = chunk.Status.Retry
		if chunk.Status.Retryable {
			blob.Status.Phase = v1alpha1.BlobPhaseRunning
			blob.Status.Progress = chunk.Status.Progress
		} else {
			utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
			blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, chunk.Status.Conditions...)
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

	if blob.Status.Total <= 0 {
		resp := chunk.Status.SourceResponse
		if resp != nil && resp.StatusCode == 200 && resp.Headers != nil {
			cl := resp.Headers["content-length"]
			if cl != "" {
				total, _ := strconv.ParseInt(cl, 10, 64)
				if total > 0 {
					blob.Status.Total = total
				}
			}
		}
	}
	return nil
}

func (c *BlobFromChunkController) fromChunks(ctx context.Context, blob *v1alpha1.Blob) error {
	mp, err := c.multipartInformer.Lister().Get(blob.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get multipart: %w", err)
		}
		mp, err = c.client.TaskV1alpha1().Multiparts().Get(ctx, blob.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("multipart %s not found", blob.Name)
			}
			return fmt.Errorf("failed to get multipart from API server: %w", err)
		}
	}

	mp = mp.DeepCopy()

	chunks, err := c.chunkInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	var updateEtag bool
	var succeededCount, failedCount, pendingCount, runningCount int64
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

		progress += chunk.Status.Progress
	}

	updateEtagFunc := func() {
		_, err = c.client.TaskV1alpha1().Multiparts().Update(ctx, mp, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update multipart %s: %v", mp.Name, err)
		}
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
		for _, chunk := range chunks {
			if chunk.Status.Phase != v1alpha1.ChunkPhaseFailed {
				continue
			}
			if chunk.Status.Retryable {
				continue
			}
			blob.Status.Retry = chunk.Status.Retry
			utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
			for _, chunk := range chunks {
				if chunk.Status.Phase == v1alpha1.ChunkPhaseFailed {
					blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, chunk.Status.Conditions...)
				}
			}
			blob.Status.RunningChunks = 0
			blob.Status.PendingChunks = 0
			c.deleteChunksInNonFinalStates(ctx, blob)
			return nil
		}
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
		if updateEtag {
			go updateEtagFunc()
		}
		return nil
	}

	if blob.Spec.ChunksNumber != succeededCount {
		blob.Status.Phase = v1alpha1.BlobPhaseRunning
		if updateEtag {
			go updateEtagFunc()
		}
		return nil
	}

	if len(mp.UploadIDs) == 0 {
		utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
		blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
			Type:    "MultipartIncomplete",
			Message: "all chunks succeeded but multipart upload IDs are missing",
		})
		blob.Status.RunningChunks = 0
		blob.Status.PendingChunks = 0
		return nil
	}

	if blob.Status.Progress != blob.Status.Total {
		utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
		blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
			Type:    "BlobSizeMismatch",
			Message: fmt.Sprintf("total size of all chunks (%d) does not match expected total (%d)", blob.Status.Progress, blob.Status.Total),
		})
		blob.Status.RunningChunks = 0
		blob.Status.PendingChunks = 0
		return nil
	}

	var totalSize int64
	for _, etag := range mp.UploadEtags {
		totalSize += etag.Size
	}
	if totalSize != blob.Status.Total {
		utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
		blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
			Type:    "UploadSizeMismatch",
			Message: fmt.Sprintf("total size of uploaded parts (%d) does not match expected total (%d)", totalSize, blob.Status.Total),
		})
		blob.Status.RunningChunks = 0
		blob.Status.PendingChunks = 0
		c.deleteChunksInNonFinalStates(ctx, blob)
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
		utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
		blob.Status.Conditions = append(blob.Status.Conditions, v1alpha1.Condition{
			Type:    "MultipartCommit",
			Message: err.Error(),
		})
		blob.Status.RunningChunks = 0
		blob.Status.PendingChunks = 0
		c.deleteChunksInNonFinalStates(ctx, blob)
		return nil
	}

	err = c.verifySha256(ctx, blob)
	if err != nil {
		utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseFailed)
		blob.Status.Conditions = v1alpha1.AppendConditions(blob.Status.Conditions, v1alpha1.Condition{
			Type:    "Sha256Verification",
			Message: err.Error(),
		})
		blob.Status.RunningChunks = 0
		blob.Status.PendingChunks = 0
		c.deleteChunksInNonFinalStates(ctx, blob)
		return nil
	}

	utils.SetBlobTerminalPhase(blob, v1alpha1.BlobPhaseSucceeded)
	err = c.client.TaskV1alpha1().Multiparts().Delete(ctx, blob.Name, metav1.DeleteOptions{})
	if err != nil {
		klog.Errorf("failed to delete multipart %s: %v", blob.Name, err)
		return nil
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

// deleteChunksInNonFinalStates deletes chunks that are in Running, Pending, or Unknown states
// when a blob fails. This ensures cleanup of incomplete work when a blob cannot be completed.
func (c *BlobFromChunkController) deleteChunksInNonFinalStates(ctx context.Context, blob *v1alpha1.Blob) {
	chunks, err := c.chunkInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		klog.Errorf("failed to list chunks for blob %s during cleanup: %v", blob.Name, err)
		return
	}

	for _, chunk := range chunks {
		// Only delete chunks in non-final states (Running, Pending, Unknown)
		if chunk.Status.Phase == v1alpha1.ChunkPhaseRunning ||
			chunk.Status.Phase == v1alpha1.ChunkPhasePending ||
			chunk.Status.Phase == v1alpha1.ChunkPhaseUnknown {
			err := c.client.TaskV1alpha1().Chunks().Delete(ctx, chunk.Name, metav1.DeleteOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					klog.Errorf("failed to delete chunk %s for failed blob %s: %v", chunk.Name, blob.Name, err)
				}
			} else {
				klog.Infof("Deleted chunk %s in %s state for failed blob %s", chunk.Name, chunk.Status.Phase, blob.Name)
			}
		}
	}
}
