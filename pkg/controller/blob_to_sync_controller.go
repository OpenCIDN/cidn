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

type BlobToSyncController struct {
	handlerName  string
	s3           map[string]*sss.SSS
	expires      time.Duration
	client       versioned.Interface
	blobInformer informers.BlobInformer
	syncInformer informers.SyncInformer
	workqueue    workqueue.TypedDelayingInterface[string]
}

func NewBlobToSyncController(
	handlerName string,
	s3 map[string]*sss.SSS,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobToSyncController {
	c := &BlobToSyncController{
		handlerName:  handlerName,
		s3:           s3,
		expires:      24 * time.Hour,
		blobInformer: sharedInformerFactory.Task().V1alpha1().Blobs(),
		syncInformer: sharedInformerFactory.Task().V1alpha1().Syncs(),
		client:       client,
		workqueue:    workqueue.NewTypedDelayingQueue[string](),
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

func (c *BlobToSyncController) Start(ctx context.Context) error {

	go c.runWorker(ctx)
	return nil
}

func (c *BlobToSyncController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BlobToSyncController) cleanupBlob(blob *v1alpha1.Blob) {
	err := c.client.TaskV1alpha1().Syncs().DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labels.Set{
			BlobUIDLabelKey: string(blob.UID),
		}.String(),
	})
	if err != nil {
		klog.Errorf("failed to delete syncs for blob %s: %v", blob.Name, err)
	}
}

func (c *BlobToSyncController) processNextItem(ctx context.Context) bool {
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

func (c *BlobToSyncController) syncHandler(ctx context.Context, name string) error {
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

	switch blob.Status.Phase {
	case v1alpha1.BlobPhaseRunning, v1alpha1.BlobPhaseUnknown:
		if blob.Spec.ChunkSize != 0 && blob.Spec.Total > blob.Spec.ChunkSize {
			err := c.toSyncs(ctx, blob)
			if err != nil {
				return fmt.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
			}

		} else {
			err := c.toOneSync(ctx, blob)
			if err != nil {
				return fmt.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
			}
		}
	case v1alpha1.BlobPhaseSucceeded:
		c.cleanupBlob(blob)
	}

	return nil
}

func (c *BlobToSyncController) toOneSync(ctx context.Context, blob *v1alpha1.Blob) error {
	existingSync, err := c.syncInformer.Lister().Get(blob.Name)
	if err == nil && existingSync != nil {
		if existingSync.Spec.Total == blob.Spec.Total &&
			existingSync.Spec.Sha256 == blob.Spec.ContentSha256 &&
			existingSync.Annotations[BlobNameAnnotationKey] == blob.Name &&
			existingSync.Labels[BlobUIDLabelKey] == string(blob.UID) {
			// Sync already exists and matches, no need to create a new one
			return nil
		}
		// Delete existing sync since it doesn't match
		err := c.client.TaskV1alpha1().Syncs().Delete(ctx, blob.Name, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete mismatched sync: %v", err)
			}
		}
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing sync: %v", err)
	}
	sync := &v1alpha1.Sync{
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
		Spec: v1alpha1.SyncSpec{
			Total:      blob.Spec.Total,
			Priority:   blob.Spec.Priority,
			Sha256:     blob.Spec.ContentSha256,
			RetryCount: blob.Spec.RetryCount - blob.Status.RetryCount,
		},
		Status: v1alpha1.SyncStatus{
			Phase: v1alpha1.SyncPhasePending,
		},
	}

	if blob.Spec.ContentSha256 != "" {
		sync.Spec.Sha256PartialPreviousName = "-"
	}

	sync.Spec.Source = v1alpha1.SyncHTTP{
		Request: v1alpha1.SyncHTTPRequest{
			Method: http.MethodGet,
			URL:    blob.Spec.Source,
		},
		Response: v1alpha1.SyncHTTPResponse{
			StatusCode: http.StatusOK,
		},
	}

	if blob.Spec.SourceEtag != "" {
		if sync.Spec.Source.Response.Headers == nil {
			sync.Spec.Source.Response.Headers = map[string]string{}
		}
		sync.Spec.Source.Response.Headers["Etag"] = blob.Spec.SourceEtag
	}

	for _, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		d, err := s3.SignPut(dst.Path, c.expires)
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

func (c *BlobToSyncController) buildSync(blob *v1alpha1.Blob, name string, num, start, end int64, lastName string, uploadIDs []string) (*v1alpha1.Sync, error) {
	apiVersion := v1alpha1.GroupVersion.String()
	sync := &v1alpha1.Sync{
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
		Spec: v1alpha1.SyncSpec{
			Total:        end - start,
			Priority:     blob.Spec.Priority,
			RetryCount:   blob.Spec.RetryCount - blob.Status.RetryCount,
			ChunkIndex:   num,
			ChunksNumber: blob.Spec.ChunksNumber,
		},
		Status: v1alpha1.SyncStatus{
			Phase: v1alpha1.SyncPhasePending,
		},
	}

	if blob.Spec.ContentSha256 != "" {
		sync.Spec.Sha256PartialPreviousName = lastName
	}

	if num == blob.Spec.ChunksNumber && blob.Spec.ContentSha256 != "" {
		sync.Spec.Sha256 = blob.Spec.ContentSha256
	}

	sync.Spec.Source = v1alpha1.SyncHTTP{
		Request: v1alpha1.SyncHTTPRequest{
			Method: http.MethodGet,
			URL:    blob.Spec.Source,
			Headers: map[string]string{
				"Range": fmt.Sprintf("bytes=%d-%d", start, end-1),
			},
		},
		Response: v1alpha1.SyncHTTPResponse{
			StatusCode: http.StatusPartialContent,
		},
	}

	if blob.Spec.SourceEtag != "" {
		if sync.Spec.Source.Response.Headers == nil {
			sync.Spec.Source.Response.Headers = map[string]string{}
		}
		sync.Spec.Source.Response.Headers["Etag"] = blob.Spec.SourceEtag
	}

	for j, dst := range blob.Spec.Destination {
		s3 := c.s3[dst.Name]
		if s3 == nil {
			return nil, fmt.Errorf("s3 client for destination %q not found", dst.Name)
		}

		mp := s3.GetMultipartWithUploadID(dst.Path, uploadIDs[j])
		partURL, err := mp.SignUploadPart(num, c.expires)
		if err != nil {
			return nil, err
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

	return sync, nil
}

func (c *BlobToSyncController) toSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
	syncs, err := c.syncInformer.Lister().List(labels.SelectorFromSet(labels.Set{
		BlobUIDLabelKey: string(blob.UID),
	}))
	if err != nil {
		return err
	}

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
	if failedCount != 0 {
		return nil
	}

	toCreate := int(blob.Spec.MaximumParallelism) - (pendingCount + runningCount)
	if toCreate <= 0 {
		return nil
	}

	if uploadIDs := blob.Status.UploadIDs; len(uploadIDs) == 0 {
		for _, dst := range blob.Spec.Destination {
			s3 := c.s3[dst.Name]
			if s3 == nil {
				return fmt.Errorf("s3 client for destination %q not found", dst.Name)
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
		blob.Status.UploadIDs = uploadIDs
	}

	created := 0
	p0 := decimalStringLength(blob.Spec.ChunksNumber)
	s0 := hexStringLength(blob.Spec.Total)
	lastName := "-"

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(int(blob.Spec.MaximumParallelism))
	for i := int64(0); i < blob.Spec.ChunksNumber && created < toCreate; i++ {
		start := i * blob.Spec.ChunkSize
		end := start + blob.Spec.ChunkSize
		if end > blob.Spec.Total {
			end = blob.Spec.Total
		}

		num := i + 1
		name := fmt.Sprintf("%s.%0*d.%0*x-%0*x", blob.Name, p0, num, s0, start, s0, end)

		if len(blob.Status.UploadEtags) != 0 && len(blob.Status.UploadEtags[i].Etags) != 0 {
			if i < int64(len(blob.Status.UploadEtags))-1 && len(blob.Status.UploadEtags[i+1].Etags) != 0 {
				if _, err := c.syncInformer.Lister().Get(name); err == nil {
					g.Go(func() error {
						err := c.client.TaskV1alpha1().Syncs().Delete(ctx, name, metav1.DeleteOptions{})
						if err != nil {
							if !apierrors.IsNotFound(err) {
								klog.Errorf("failed to delete existing sync %s: %v", name, err)
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

		if _, err := c.syncInformer.Lister().Get(name); err == nil {
			lastName = name
			continue
		} else if !apierrors.IsNotFound(err) {
			return err
		}

		sync, err := c.buildSync(blob, name, num, start, end, lastName, blob.Status.UploadIDs)
		if err != nil {
			return err
		}

		g.Go(func() error {
			_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
			if err == nil {
				return nil
			}

			if !apierrors.IsAlreadyExists(err) {
				klog.Errorf("failed to create sync %s: %v", sync.Name, err)
				return nil
			}

			// Delete existing sync and retry
			err := c.client.TaskV1alpha1().Syncs().Delete(ctx, sync.Name, metav1.DeleteOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					klog.Errorf("failed to delete existing sync %s: %v", sync.Name, err)
					return nil
				}
			}
			_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
			if err != nil {
				klog.Errorf("failed to create sync %s after retry: %v", sync.Name, err)
				return nil
			}

			return nil
		})

		created++
		lastName = name
	}

	return nil
}
