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
	"math"
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
	s3           *sss.SSS
	expires      time.Duration
	client       versioned.Interface
	blobInformer informers.BlobInformer
	syncInformer informers.SyncInformer
	workqueue    workqueue.TypedDelayingInterface[string]
}

func NewBlobToSyncController(
	handlerName string,
	s3 *sss.SSS,
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
			c.cleanupBlob(obj)
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

func (c *BlobToSyncController) cleanupBlob(obj interface{}) {
	blob, ok := obj.(*v1alpha1.Blob)
	if !ok {
		return
	}

	err := c.client.TaskV1alpha1().Syncs().DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labels.Set{
			BlobNameLabel: blob.Name,
			BlobUIDLabel:  string(blob.UID),
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

func (c *BlobToSyncController) syncHandler(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("invalid resource key: %s", key)
		return nil
	}

	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		return err
	}

	if blob.Spec.HandlerName != c.handlerName {
		return nil
	}

	if blob.Status.Phase != v1alpha1.BlobPhaseRunning {
		return nil
	}

	if blob.Spec.ChunkSize != 0 && blob.Spec.Total > blob.Spec.ChunkSize {
		err := c.createSyncs(ctx, blob)
		if err != nil {
			return fmt.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
		}

	} else {
		err := c.createOneSync(ctx, blob)
		if err != nil {
			return fmt.Errorf("failed to create sync for blob %s: %v", blob.Name, err)
		}
	}

	return nil
}

func (c *BlobToSyncController) createOneSync(ctx context.Context, blob *v1alpha1.Blob) error {
	existingSync, err := c.syncInformer.Lister().Get(blob.Name)
	if err == nil && existingSync != nil {
		if existingSync.Spec.Total == blob.Spec.Total &&
			existingSync.Spec.Sha256 == blob.Spec.Sha256 &&
			existingSync.Labels[BlobNameLabel] == blob.Name &&
			existingSync.Labels[BlobUIDLabel] == string(blob.UID) {
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
				BlobNameLabel: blob.Name,
				BlobUIDLabel:  string(blob.UID),
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
			Total:    blob.Spec.Total,
			Priority: blob.Spec.Priority,
			Sha256:   blob.Spec.Sha256,
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
		Response: v1alpha1.SyncHTTPResponse{
			StatusCode: http.StatusOK,
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

func (c *BlobToSyncController) createSyncs(ctx context.Context, blob *v1alpha1.Blob) error {
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
		if sync.Labels[BlobUIDLabel] != string(blob.UID) {
			// Delete mismatched sync
			if err := c.client.TaskV1alpha1().Syncs().Delete(ctx, sync.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete existing sync: %v", err)
			}
			continue
		}

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
	if failedCount != 0 {
		return nil
	}

	// Calculate how many new syncs we can create
	toCreate := int(blob.Spec.MaximumParallelism) - (pendingCount + runningCount)
	if toCreate <= 0 {
		return nil
	}

	if uploadIDs := blob.Status.UploadIDs; len(uploadIDs) == 0 {
		for _, dst := range blob.Spec.Destination {
			// TODO: When testing with minio, creating multipart uploads with the same path
			// may sometimes return success but actually fail to create the part.
			// This appears to be a minio-specific issue.
			mp, err := c.s3.GetMultipart(ctx, dst)
			if err != nil {
				mp, err = c.s3.NewMultipart(ctx, dst)
				if err != nil {
					return err
				}
			}
			uploadIDs = append(uploadIDs, mp.UploadID())
		}

		blob.Status.UploadIDs = uploadIDs
	}

	created := 0

	apiVersion := v1alpha1.GroupVersion.String()

	p0 := int(math.Log10(float64(blob.Spec.ChunksNumber+1)) + 1)
	s0 := int(math.Log10(float64(blob.Spec.Total)) + 1)
	lastName := "-"

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for i := int64(0); i < blob.Spec.ChunksNumber && created < toCreate; i++ {
		start := i * blob.Spec.ChunkSize
		end := start + blob.Spec.ChunkSize
		if end > blob.Spec.Total {
			end = blob.Spec.Total
		}

		num := i + 1
		name := fmt.Sprintf("%s.%0*d.%0*d-%0*d", blob.Name, p0, num, s0, start, s0, end)

		sync, err := c.syncInformer.Lister().Get(name)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			// Not found - we'll create new sync below
		} else {
			lastName = name
			continue
		}

		sync = &v1alpha1.Sync{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					BlobNameLabel: blob.Name,
					BlobUIDLabel:  string(blob.UID),
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
				ChunkIndex:   num,
				ChunksNumber: blob.Spec.ChunksNumber,
			},
			Status: v1alpha1.SyncStatus{
				Phase: v1alpha1.SyncPhasePending,
			},
		}

		if blob.Spec.Sha256 != "" {
			sync.Spec.Sha256PartialPreviousName = lastName
		}

		if num == blob.Spec.ChunksNumber && blob.Spec.Sha256 != "" {
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
			Response: v1alpha1.SyncHTTPResponse{
				StatusCode: http.StatusPartialContent,
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

		g.Go(func() error {
			_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
			if err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return err
				}

				// Delete existing sync and retry
				err := c.client.TaskV1alpha1().Syncs().Delete(ctx, sync.Name, metav1.DeleteOptions{})
				if err != nil {
					if !apierrors.IsNotFound(err) {
						return fmt.Errorf("failed to delete existing sync: %v", err)
					}
				}
				_, err = c.client.TaskV1alpha1().Syncs().Create(ctx, sync, metav1.CreateOptions{})
				if err != nil {
					return err
				}
			}
			return nil
		})

		created++
		lastName = name
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	return nil
}
