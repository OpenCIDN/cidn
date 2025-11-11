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
	"net/http"
	"net/textproto"
	"slices"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	"github.com/wzshiming/sss"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type BlobToChunkController struct {
	handlerName       string
	s3                map[string]*sss.SSS
	expires           time.Duration
	client            versioned.Interface
	blobInformer      informers.BlobInformer
	chunkInformer     informers.ChunkInformer
	multipartInformer informers.MultipartInformer
	workqueue         workqueue.TypedDelayingInterface[string]
}

func NewBlobToChunkController(
	handlerName string,
	s3 map[string]*sss.SSS,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobToChunkController {
	c := &BlobToChunkController{
		handlerName:       handlerName,
		s3:                s3,
		expires:           24 * time.Hour,
		blobInformer:      sharedInformerFactory.Task().V1alpha1().Blobs(),
		chunkInformer:     sharedInformerFactory.Task().V1alpha1().Chunks(),
		multipartInformer: sharedInformerFactory.Task().V1alpha1().Multiparts(),
		client:            client,
		workqueue:         workqueue.NewTypedDelayingQueue[string](),
	}

	c.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			blob := obj.(*v1alpha1.Blob)
			key := blob.Name
			c.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			blob := newObj.(*v1alpha1.Blob)
			key := blob.Name
			c.workqueue.Add(key)
		},
		DeleteFunc: func(obj interface{}) {
			blob, ok := obj.(*v1alpha1.Blob)
			if !ok {
				return
			}

			c.cleanupBlob(blob)
		},
	})

	c.multipartInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			multipart := obj.(*v1alpha1.Multipart)
			key := multipart.Name
			c.workqueue.Add(key)
		},
	})
	return c
}

func (c *BlobToChunkController) Start(ctx context.Context) error {

	go c.runWorker(ctx)
	return nil
}

func (c *BlobToChunkController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BlobToChunkController) cleanupBlob(blob *v1alpha1.Blob) {
	err := c.client.TaskV1alpha1().Chunks().DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labels.Set{
			BlobUIDLabelKey: string(blob.UID),
		}.String(),
	})
	if err != nil {
		klog.Errorf("failed to delete chunks for blob %s: %v", blob.Name, err)
	}

	_, err = c.multipartInformer.Lister().Get(blob.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("failed to get multipart for blob %s: %v", blob.Name, err)
		}
	} else {
		err = c.client.TaskV1alpha1().Multiparts().Delete(context.Background(), blob.Name, metav1.DeleteOptions{})
		if err != nil {
			klog.Errorf("failed to delete chunks for blob %s: %v", blob.Name, err)
		}
	}
}

func (c *BlobToChunkController) processNextItem(ctx context.Context) bool {
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

func (c *BlobToChunkController) chunkHandler(ctx context.Context, name string) error {
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

	if blob.Status.Total == 0 {
		// Optimization: If ChunksNumber is 1, skip HEAD request and directly GET/PUT
		if blob.Spec.ChunksNumber == 1 {
			return c.toOneChunk(ctx, blob)
		}
		return c.toHeadChunk(ctx, blob)
	}

	switch blob.Status.Phase {
	case v1alpha1.BlobPhaseRunning, v1alpha1.BlobPhaseUnknown:
		if blob.Spec.ChunksNumber != 0 {
			if blob.Spec.ChunksNumber != 1 && blob.Status.AcceptRanges {
				err := c.toChunks(ctx, blob)
				if err != nil {
					return fmt.Errorf("failed to create chunk for blob %s: %v", blob.Name, err)
				}

			} else {
				err := c.toOneChunk(ctx, blob)
				if err != nil {
					return fmt.Errorf("failed to create chunk for blob %s: %v", blob.Name, err)
				}
			}
		}

	case v1alpha1.BlobPhaseSucceeded:
		c.cleanupBlob(blob)
	}

	return nil
}

func buildHeadChunkName(blobName string, i int) string {
	return fmt.Sprintf("blob:head:%s:%d", blobName, i)
}

func (c *BlobToChunkController) toHeadChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	chunkName := buildHeadChunkName(blob.Name, 0)
	existingChunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err == nil && existingChunk != nil {
		if existingChunk.Annotations[BlobNameAnnotationKey] == blob.Name &&
			existingChunk.Labels[BlobUIDLabelKey] == string(blob.UID) {
			return nil
		}

		err := c.client.TaskV1alpha1().Chunks().Delete(ctx, chunkName, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete mismatched chunk: %v", err)
			}
		}
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing chunk: %v", err)
	}

	src := blob.Spec.Source[0]

	headers := map[string]string{}

	for k, v := range src.Headers {
		headers[textproto.CanonicalMIMEHeaderKey(k)] = v
	}

	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name: chunkName,
			Labels: map[string]string{
				BlobUIDLabelKey: string(blob.UID),
			},
			Annotations: map[string]string{
				BlobNameAnnotationKey: blob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       v1alpha1.BlobKind,
					Name:       blob.Name,
					UID:        blob.UID,
				},
			},
		},
		Spec: v1alpha1.ChunkSpec{
			Priority:     blob.Spec.Priority,
			MaximumRetry: blob.Spec.MaximumRetry - blob.Status.Retry,
		},
		Status: v1alpha1.ChunkStatus{
			Phase: v1alpha1.ChunkPhasePending,
		},
	}

	chunk.Spec.BearerName = src.BearerName
	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request: v1alpha1.ChunkHTTPRequest{
			Method:  http.MethodHead,
			URL:     src.URL,
			Headers: headers,
		},
		Response: v1alpha1.ChunkHTTPResponse{
			StatusCode: http.StatusOK,
		},
	}

	_, err = c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

func buildFullChunkName(blobName string) string {
	return fmt.Sprintf("blob:full:%s", blobName)
}

func (c *BlobToChunkController) toOneChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	chunkName := buildFullChunkName(blob.Name)
	existingChunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err == nil && existingChunk != nil {
		if existingChunk.Spec.Total == blob.Status.Total &&
			existingChunk.Spec.Sha256 == blob.Spec.ContentSha256 &&
			existingChunk.Annotations[BlobNameAnnotationKey] == blob.Name &&
			existingChunk.Labels[BlobUIDLabelKey] == string(blob.UID) {
			// Chunk already exists and matches, no need to create a new one
			return nil
		}
		// Delete existing chunk since it doesn't match
		err := c.client.TaskV1alpha1().Chunks().Delete(ctx, blob.Name, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete mismatched chunk: %v", err)
			}
		}
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing chunk: %v", err)
	}

	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name: chunkName,
			Labels: map[string]string{
				BlobUIDLabelKey: string(blob.UID),
			},
			Annotations: map[string]string{
				BlobNameAnnotationKey: blob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       v1alpha1.BlobKind,
					Name:       blob.Name,
					UID:        blob.UID,
				},
			},
		},
		Spec: v1alpha1.ChunkSpec{
			Total:        blob.Status.Total,
			Priority:     blob.Spec.Priority,
			Sha256:       blob.Spec.ContentSha256,
			MaximumRetry: blob.Spec.MaximumRetry - blob.Status.Retry,
		},
		Status: v1alpha1.ChunkStatus{
			Phase: v1alpha1.ChunkPhasePending,
		},
	}

	if blob.Spec.ContentSha256 != "" {
		chunk.Spec.Sha256PartialPreviousName = "-"
	}

	src := blob.Spec.Source[0]

	headers := map[string]string{}

	for k, v := range src.Headers {
		headers[textproto.CanonicalMIMEHeaderKey(k)] = v
	}

	chunk.Spec.BearerName = src.BearerName
	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request: v1alpha1.ChunkHTTPRequest{
			Method:  http.MethodGet,
			URL:     src.URL,
			Headers: headers,
		},
		Response: v1alpha1.ChunkHTTPResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{},
		},
	}

	for _, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		// Only check SkipIfExists when Total is known
		if dst.SkipIfExists && blob.Status.Total > 0 {
			fi, err := s3.StatHead(ctx, dst.Path)
			if err == nil && fi.Size() == blob.Status.Total {
				continue
			}
		}

		d, err := s3.SignPut(dst.Path, c.expires)
		if err != nil {
			return err
		}

		dest := v1alpha1.ChunkHTTP{
			Request: v1alpha1.ChunkHTTPRequest{
				Method:  http.MethodPut,
				URL:     d,
				Headers: map[string]string{},
			},
			Response: v1alpha1.ChunkHTTPResponse{
				StatusCode: http.StatusOK,
			},
		}
		if blob.Status.Total > 0 {
			dest.Request.Headers["Content-Length"] = fmt.Sprintf("%d", blob.Status.Total)
		}

		chunk.Spec.Destination = append(chunk.Spec.Destination, dest)

	}

	if len(chunk.Spec.Destination) == 0 {
		_, err := utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Blobs(), blob, func(b *v1alpha1.Blob) *v1alpha1.Blob {
			b.Status.SucceededChunks = 1
			b.Status.FailedChunks = 0
			b.Status.PendingChunks = 0
			b.Status.RunningChunks = 0
			utils.SetBlobTerminalPhase(b, v1alpha1.BlobPhaseSucceeded)
			b.Status.Progress = b.Status.Total
			return b
		})
		if err != nil {
			return fmt.Errorf("failed to update blob status: %v", err)
		}
		return nil
	}

	_, err = c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

func (c *BlobToChunkController) buildChunk(blob *v1alpha1.Blob, name string, num, start, end int64, lastName string, uploadIDs []string) (*v1alpha1.Chunk, error) {
	apiVersion := v1alpha1.GroupVersion.String()
	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				BlobUIDLabelKey: string(blob.UID),
			},
			Annotations: map[string]string{
				BlobNameAnnotationKey: blob.Name,
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
		Spec: v1alpha1.ChunkSpec{
			Total:        end - start,
			Priority:     blob.Spec.Priority,
			MaximumRetry: blob.Spec.MaximumRetry - blob.Status.Retry,
			ChunkIndex:   num,
			ChunksNumber: blob.Spec.ChunksNumber,
		},
		Status: v1alpha1.ChunkStatus{
			Phase: v1alpha1.ChunkPhasePending,
		},
	}

	group := blob.Annotations[v1alpha1.WebuiGroupAnnotation]
	if group != "" {
		chunk.Annotations[v1alpha1.WebuiGroupAnnotation] = group
		chunk.Annotations[v1alpha1.WebuiGroupIgnoreSizeAnnotation] = "true"
	}

	blobName := blob.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
	if blobName == "" {
		blobName = blob.Name
	}
	chunk.Annotations[v1alpha1.WebuiDisplayNameAnnotation] = fmt.Sprintf("Part %d of %s", num, blobName)

	if blob.Spec.ContentSha256 != "" {
		chunk.Spec.Sha256PartialPreviousName = lastName
	}

	if num == blob.Spec.ChunksNumber && blob.Spec.ContentSha256 != "" {
		chunk.Spec.Sha256 = blob.Spec.ContentSha256
	}

	src := blob.Spec.Source[num%int64(len(blob.Spec.Source))]

	headers := map[string]string{
		"Range": fmt.Sprintf("bytes=%d-%d", start, end-1),
	}

	for k, v := range src.Headers {
		headers[textproto.CanonicalMIMEHeaderKey(k)] = v
	}

	chunk.Spec.BearerName = src.BearerName
	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request: v1alpha1.ChunkHTTPRequest{
			Method:  http.MethodGet,
			URL:     src.URL,
			Headers: headers,
		},
		Response: v1alpha1.ChunkHTTPResponse{
			StatusCode: http.StatusPartialContent,
			Headers: map[string]string{
				"Content-Range": fmt.Sprintf("bytes %d-%d/%d", start, end-1, blob.Status.Total),
			},
		},
	}

	for j, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return nil, fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		uploadID := uploadIDs[j]
		if uploadID == "" {
			chunk.Spec.Destination = append(chunk.Spec.Destination, v1alpha1.ChunkHTTP{
				Request: v1alpha1.ChunkHTTPRequest{
					Method: "",
					URL:    "",
				},
			})
			continue
		}

		mp := s3.GetMultipartWithUploadID(dst.Path, uploadID)
		partURL, err := mp.SignUploadPart(num, c.expires)
		if err != nil {
			return nil, err
		}

		dest := v1alpha1.ChunkHTTP{
			Request: v1alpha1.ChunkHTTPRequest{
				Method:  http.MethodPut,
				URL:     partURL,
				Headers: map[string]string{},
			},
			Response: v1alpha1.ChunkHTTPResponse{
				StatusCode: http.StatusOK,
			},
		}
		if blob.Status.Total > 0 {
			dest.Request.Headers["Content-Length"] = fmt.Sprintf("%d", blob.Status.Total)
		}

		chunk.Spec.Destination = append(chunk.Spec.Destination, dest)
	}

	return chunk, nil
}

func (c *BlobToChunkController) toMultipart(ctx context.Context, blob *v1alpha1.Blob) (*v1alpha1.Multipart, error) {
	destinationNames := make([]string, 0, len(blob.Spec.Destination))
	uploadIDs := make([]string, 0, len(blob.Spec.Destination))
	for _, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return nil, fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		destinationNames = append(destinationNames, dst.Name)

		if dst.SkipIfExists {
			fi, err := s3.StatHead(ctx, dst.Path)
			if err == nil && fi.Size() == blob.Status.Total {
				uploadIDs = append(uploadIDs, "")
				continue
			}
		}

		mp, err := s3.GetMultipart(ctx, dst.Path)
		if err != nil {
			mp, err = s3.NewMultipart(ctx, dst.Path)
			if err != nil {
				return nil, err
			}
		}
		uploadIDs = append(uploadIDs, mp.UploadID())
	}

	mp := &v1alpha1.Multipart{
		ObjectMeta: metav1.ObjectMeta{
			Name: blob.Name,
		},
		DestinationNames: destinationNames,
		UploadIDs:        uploadIDs,
		UploadEtags:      make([]v1alpha1.UploadEtags, blob.Spec.ChunksNumber),
	}

	mp, err := c.client.TaskV1alpha1().Multiparts().Create(ctx, mp, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart: %v", err)
	}

	return mp, nil
}

// chunkCounts holds the count of chunks in different phases
type chunkCounts struct {
	pending int
	running int
	failed  int
}

// countChunksByPhase counts chunks grouped by their phase
func countChunksByPhase(chunks []*v1alpha1.Chunk) chunkCounts {
	counts := chunkCounts{}
	for _, chunk := range chunks {
		switch chunk.Status.Phase {
		case v1alpha1.ChunkPhasePending:
			counts.pending++
		case v1alpha1.ChunkPhaseRunning:
			counts.running++
		case v1alpha1.ChunkPhaseFailed:
			counts.failed++
		}
	}
	return counts
}

// calculateChunksToCreate determines how many chunks should be created
func calculateChunksToCreate(blob *v1alpha1.Blob, counts chunkCounts) int {
	if counts.failed != 0 {
		return 0
	}

	if counts.pending >= int(blob.Spec.MaximumPending) {
		return 0
	}

	toCreate := int(blob.Spec.MaximumRunning) - (counts.pending + counts.running)
	if toCreate <= 0 {
		return 0
	}

	if toCreate > int(blob.Spec.MaximumPending) {
		toCreate = int(blob.Spec.MaximumPending)
	}

	return toCreate
}

// getOrCreateMultipart retrieves an existing multipart or creates a new one
func (c *BlobToChunkController) getOrCreateMultipart(ctx context.Context, blob *v1alpha1.Blob) (*v1alpha1.Multipart, error) {
	mp, err := c.multipartInformer.Lister().Get(blob.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		return c.toMultipart(ctx, blob)
	}

	// Validate existing multipart matches current blob configuration
	needsRecreation, err := c.needsMultipartRecreation(blob, mp)
	if err != nil {
		return nil, err
	}
	if needsRecreation {
		if err := c.client.TaskV1alpha1().Multiparts().Delete(ctx, blob.Name, metav1.DeleteOptions{}); err != nil {
			return nil, err
		}
		return c.toMultipart(ctx, blob)
	}

	return mp, nil
}

// needsMultipartRecreation checks if multipart needs to be recreated
func (c *BlobToChunkController) needsMultipartRecreation(blob *v1alpha1.Blob, mp *v1alpha1.Multipart) (bool, error) {
	destinationNames := make([]string, 0, len(blob.Spec.Destination))
	for _, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return false, fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}
		destinationNames = append(destinationNames, dst.Name)
	}

	needsRecreation := !slices.Equal(destinationNames, mp.DestinationNames) ||
		blob.Spec.ChunksNumber != int64(len(mp.UploadEtags))
	return needsRecreation, nil
}

// scheduleChunkDeletion schedules a succeeded chunk for deletion
func (c *BlobToChunkController) scheduleChunkDeletion(ctx context.Context, name string) error {
	err := c.client.TaskV1alpha1().Chunks().Delete(ctx, name, metav1.DeleteOptions{})
	if err == nil {
		return nil
	}

	if apierrors.IsNotFound(err) {
		return nil
	}

	return err
}

// scheduleChunkCreation schedules a chunk for creation
func (c *BlobToChunkController) scheduleChunkCreation(ctx context.Context, chunk *v1alpha1.Chunk) error {
	_, err := c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
	if err == nil {
		return nil
	}

	if apierrors.IsAlreadyExists(err) {
		return nil
	}

	return err
}

func (c *BlobToChunkController) toChunks(ctx context.Context, blob *v1alpha1.Blob) error {
	// Get existing chunks and count by phase
	chunks, err := c.chunkInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		return err
	}

	counts := countChunksByPhase(chunks)
	toCreate := calculateChunksToCreate(blob, counts)
	if toCreate == 0 {
		return nil
	}

	// Get or create multipart upload
	mp, err := c.getOrCreateMultipart(ctx, blob)
	if err != nil {
		return err
	}
	if mp == nil {
		return nil
	}

	// Create chunks
	return c.createChunks(ctx, blob, mp, toCreate)
}

// shouldSkipChunk checks if a chunk should be skipped (already uploaded)
func shouldSkipChunk(mp *v1alpha1.Multipart, chunkIndex int64) bool {
	if len(mp.UploadEtags) == 0 {
		return false
	}
	if chunkIndex >= int64(len(mp.UploadEtags)) {
		return false
	}
	return len(mp.UploadEtags[chunkIndex].Etags) != 0
}

// createChunks creates the specified number of chunks for the blob
func (c *BlobToChunkController) createChunks(ctx context.Context, blob *v1alpha1.Blob, mp *v1alpha1.Multipart, toCreate int) error {
	created := 0
	p0 := decimalStringLength(blob.Spec.ChunksNumber)
	s0 := hexStringLength(blob.Status.Total)
	lastName := "-"

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(toCreate)

	for i := int64(0); i < blob.Spec.ChunksNumber && created < toCreate; i++ {
		start := i * blob.Spec.ChunkSize
		end := start + blob.Spec.ChunkSize
		if end > blob.Status.Total {
			end = blob.Status.Total
		}

		num := i + 1
		name := fmt.Sprintf("blob:part:%s:%0*d:%0*x-%0*x", blob.Name, p0, num, s0, start, s0, end)

		// Skip if chunk is already uploaded
		if shouldSkipChunk(mp, i) {
			// Schedule deletion if this is not the last pending chunk
			if shouldSkipChunk(mp, i+1) {
				chunk, err := c.chunkInformer.Lister().Get(name)
				if err == nil && chunk.Status.Phase == v1alpha1.ChunkPhaseSucceeded {
					g.Go(func() error {
						return c.scheduleChunkDeletion(ctx, name)
					})
				}
			}
			lastName = name
			continue
		}

		// Skip if chunk already exists
		if _, err := c.chunkInformer.Lister().Get(name); err == nil {
			lastName = name
			continue
		} else if !apierrors.IsNotFound(err) {
			return err
		}

		// Build and schedule chunk creation
		chunk, err := c.buildChunk(blob, name, num, start, end, lastName, mp.UploadIDs)
		if err != nil {
			return err
		}

		g.Go(func() error {
			return c.scheduleChunkCreation(ctx, chunk)
		})
		created++
		lastName = name
	}

	return nil
}
