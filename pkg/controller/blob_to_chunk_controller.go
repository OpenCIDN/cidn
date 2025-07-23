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
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
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
	handlerName   string
	s3            map[string]*sss.SSS
	expires       time.Duration
	client        versioned.Interface
	blobInformer  informers.BlobInformer
	chunkInformer informers.ChunkInformer
	workqueue     workqueue.TypedDelayingInterface[string]
}

func NewBlobToChunkController(
	handlerName string,
	s3 map[string]*sss.SSS,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobToChunkController {
	c := &BlobToChunkController{
		handlerName:   handlerName,
		s3:            s3,
		expires:       24 * time.Hour,
		blobInformer:  sharedInformerFactory.Task().V1alpha1().Blobs(),
		chunkInformer: sharedInformerFactory.Task().V1alpha1().Chunks(),
		client:        client,
		workqueue:     workqueue.NewTypedDelayingQueue[string](),
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

	switch blob.Status.Phase {
	case v1alpha1.BlobPhaseRunning, v1alpha1.BlobPhaseUnknown:
		if blob.Spec.ChunksNumber != 0 {
			if blob.Spec.ChunksNumber != 1 {
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

func (c *BlobToChunkController) toOneChunk(ctx context.Context, blob *v1alpha1.Blob) error {
	existingChunk, err := c.chunkInformer.Lister().Get(blob.Name)
	if err == nil && existingChunk != nil {
		if existingChunk.Spec.Total == blob.Spec.Total &&
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
			Name: blob.Name,
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
			Total:      blob.Spec.Total,
			Priority:   blob.Spec.Priority,
			Sha256:     blob.Spec.ContentSha256,
			RetryCount: blob.Spec.RetryCount - blob.Status.RetryCount,
		},
		Status: v1alpha1.ChunkStatus{
			Phase: v1alpha1.ChunkPhasePending,
		},
	}

	if blob.Spec.ContentSha256 != "" {
		chunk.Spec.Sha256PartialPreviousName = "-"
	}

	src := blob.Spec.Source[0]

	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request: v1alpha1.ChunkHTTPRequest{
			Method: http.MethodGet,
			URL:    src.URL,
		},
		Response: v1alpha1.ChunkHTTPResponse{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Length": fmt.Sprintf("%d", blob.Spec.Total),
			},
		},
	}

	if src.Etag != "" {
		chunk.Spec.Source.Response.Headers["Etag"] = src.Etag
	}

	for _, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		if dst.SkipIfExists {
			fi, err := s3.StatHead(ctx, dst.Path)
			if err == nil && fi.Size() == blob.Spec.Total {
				continue
			}
		}

		d, err := s3.SignPut(dst.Path, c.expires)
		if err != nil {
			return err
		}

		chunk.Spec.Destination = append(chunk.Spec.Destination, v1alpha1.ChunkHTTP{
			Request: v1alpha1.ChunkHTTPRequest{
				Method: http.MethodPut,
				URL:    d,
				Headers: map[string]string{
					"Content-Length": fmt.Sprintf("%d", blob.Spec.Total),
				},
			},
			Response: v1alpha1.ChunkHTTPResponse{
				StatusCode: http.StatusOK,
			},
		})
	}

	if len(chunk.Spec.Destination) == 0 {
		blob.Status.Phase = v1alpha1.BlobPhaseSucceeded
		blob.Status.Progress = blob.Spec.Total
		_, err := c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update blob status: %v", err)
		}
		return nil
	}

	_, err = c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
	if err != nil {
		return err
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
			RetryCount:   blob.Spec.RetryCount - blob.Status.RetryCount,
			ChunkIndex:   num,
			ChunksNumber: blob.Spec.ChunksNumber,
		},
		Status: v1alpha1.ChunkStatus{
			Phase: v1alpha1.ChunkPhasePending,
		},
	}

	if blob.Spec.ContentSha256 != "" {
		chunk.Spec.Sha256PartialPreviousName = lastName
	}

	if num == blob.Spec.ChunksNumber && blob.Spec.ContentSha256 != "" {
		chunk.Spec.Sha256 = blob.Spec.ContentSha256
	}

	src := blob.Spec.Source[num%int64(len(blob.Spec.Source))]

	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request: v1alpha1.ChunkHTTPRequest{
			Method: http.MethodGet,
			URL:    src.URL,
			Headers: map[string]string{
				"Range": fmt.Sprintf("bytes=%d-%d", start, end-1),
			},
		},
		Response: v1alpha1.ChunkHTTPResponse{
			StatusCode: http.StatusPartialContent,
			Headers: map[string]string{
				"Content-Length": fmt.Sprintf("%d", end-start),
				"Content-Range":  fmt.Sprintf("bytes %d-%d/%d", start, end-1, blob.Spec.Total),
			},
		},
	}

	if src.Etag != "" {
		chunk.Spec.Source.Response.Headers["Etag"] = src.Etag
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

		chunk.Spec.Destination = append(chunk.Spec.Destination, v1alpha1.ChunkHTTP{
			Request: v1alpha1.ChunkHTTPRequest{
				Method: http.MethodPut,
				URL:    partURL,
				Headers: map[string]string{
					"Content-Length": fmt.Sprintf("%d", end-start),
				},
			},
			Response: v1alpha1.ChunkHTTPResponse{
				StatusCode: http.StatusOK,
			},
		})
	}

	return chunk, nil
}

func (c *BlobToChunkController) toChunks(ctx context.Context, blob *v1alpha1.Blob) error {
	chunks, err := c.chunkInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		return err
	}

	pendingCount := 0
	runningCount := 0
	failedCount := 0
	for _, chunk := range chunks {
		switch chunk.Status.Phase {
		case v1alpha1.ChunkPhasePending:
			pendingCount++
		case v1alpha1.ChunkPhaseRunning:
			runningCount++
		case v1alpha1.ChunkPhaseFailed:
			failedCount++
		}
	}
	if failedCount != 0 {
		return nil
	}

	if pendingCount >= int(blob.Spec.MaximumPending) {
		return nil
	}

	toCreate := int(blob.Spec.MaximumRunning) - (pendingCount + runningCount)
	if toCreate <= 0 {
		return nil
	}

	if toCreate > int(blob.Spec.MaximumPending) {
		toCreate = int(blob.Spec.MaximumPending)
	}

	if uploadIDs := blob.Status.UploadIDs; len(uploadIDs) == 0 {
		for _, dst := range blob.Spec.Destination {
			s3 := c.s3[dst.Name]
			if s3 == nil {
				return fmt.Errorf("s3 client for destination %q not found", dst.Name)
			}

			if dst.SkipIfExists {
				fi, err := s3.StatHead(ctx, dst.Path)
				if err == nil && fi.Size() == blob.Spec.Total {
					uploadIDs = append(uploadIDs, "")
					continue
				}
			}

			mp, err := s3.GetMultipart(ctx, dst.Path)
			if err != nil {
				mp, err = s3.NewMultipart(ctx, dst.Path)
				if err != nil {
					return err
				}
			}
			uploadIDs = append(uploadIDs, mp.UploadID())
		}

		allEmpty := true
		for _, id := range uploadIDs {
			if id != "" {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			blob.Status.Phase = v1alpha1.BlobPhaseSucceeded
			blob.Status.Progress = blob.Spec.Total
			_, err := c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to update blob status: %v", err)
			}
			return nil
		}

		blob.Status.UploadIDs = uploadIDs
	}

	created := 0
	p0 := decimalStringLength(blob.Spec.ChunksNumber)
	s0 := hexStringLength(blob.Spec.Total)
	lastName := "-"

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(toCreate)
	for i := int64(0); i < blob.Spec.ChunksNumber && created < toCreate; i++ {
		start := i * blob.Spec.ChunkSize
		end := start + blob.Spec.ChunkSize
		if end > blob.Spec.Total {
			end = blob.Spec.Total
		}

		num := i + 1
		name := fmt.Sprintf("%s:%0*d:%0*x-%0*x", blob.Name, p0, num, s0, start, s0, end)

		if len(blob.Status.UploadEtags) != 0 && len(blob.Status.UploadEtags[i].Etags) != 0 {
			if i < int64(len(blob.Status.UploadEtags))-1 && len(blob.Status.UploadEtags[i+1].Etags) != 0 {
				if _, err := c.chunkInformer.Lister().Get(name); err == nil {
					g.Go(func() error {
						err := c.client.TaskV1alpha1().Chunks().Delete(ctx, name, metav1.DeleteOptions{})
						if err != nil {
							if !apierrors.IsNotFound(err) {
								klog.Errorf("failed to delete existing chunk %s: %v", name, err)
								return nil
							}
						}
						return nil
					})
				}
			}

			lastName = name
			continue
		}

		if _, err := c.chunkInformer.Lister().Get(name); err == nil {
			lastName = name
			continue
		} else if !apierrors.IsNotFound(err) {
			return err
		}

		chunk, err := c.buildChunk(blob, name, num, start, end, lastName, blob.Status.UploadIDs)
		if err != nil {
			return err
		}

		g.Go(func() error {
			_, err = c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
			if err == nil {
				return nil
			}

			if !apierrors.IsAlreadyExists(err) {
				klog.Errorf("failed to create chunk %s: %v", chunk.Name, err)
				return nil
			}

			// Delete existing chunk and retry
			err := c.client.TaskV1alpha1().Chunks().Delete(ctx, chunk.Name, metav1.DeleteOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					klog.Errorf("failed to delete existing chunk %s: %v", chunk.Name, err)
					return nil
				}
			}
			_, err = c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
			if err != nil {
				klog.Errorf("failed to create chunk %s after retry: %v", chunk.Name, err)
				return nil
			}

			return nil
		})

		created++
		lastName = name
	}

	return nil
}
