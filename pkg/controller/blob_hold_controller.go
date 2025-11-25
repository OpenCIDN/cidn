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
	"sync"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type BlobHoldController struct {
	handlerName  string
	client       versioned.Interface
	blobInformer informers.BlobInformer
	workqueue    workqueue.TypedDelayingInterface[string]
}

func NewBlobHoldController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobHoldController {
	c := &BlobHoldController{
		handlerName:  handlerName,
		blobInformer: sharedInformerFactory.Task().V1alpha1().Blobs(),
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
	})

	return c
}

func (c *BlobHoldController) ReleaseAll(ctx context.Context) error {
	blobs, err := c.blobInformer.Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	var wg sync.WaitGroup

	for _, blob := range blobs {
		if blob.Status.HandlerName != c.handlerName {
			continue
		}

		if blob.Status.Phase != v1alpha1.BlobPhasePending && blob.Status.Phase != v1alpha1.BlobPhaseRunning && blob.Status.Phase != v1alpha1.BlobPhaseUnknown {
			continue
		}

		wg.Add(1)
		go func(blob *v1alpha1.Blob) {
			defer wg.Done()

			_, err := utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Blobs(), blob, func(blob *v1alpha1.Blob) *v1alpha1.Blob {
				blob.Status.HandlerName = ""
				blob.Status.Phase = v1alpha1.BlobPhasePending
				blob.Status.Conditions = nil
				return blob
			})
			if err != nil {
				klog.Errorf("failed to release blob %s: %v", blob.Name, err)
			}
		}(blob)
	}

	wg.Wait()

	return nil
}

func (c *BlobHoldController) Shutdown(ctx context.Context) error {
	return c.ReleaseAll(ctx)
}

func (c *BlobHoldController) Start(ctx context.Context) error {
	go c.runWorker(ctx)
	return nil
}

func (c *BlobHoldController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BlobHoldController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	c.handler(ctx, key)

	return true
}

func (c *BlobHoldController) handler(ctx context.Context, name string) {
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

	if blob.Status.HandlerName != "" {
		return
	}

	if blob.Status.Phase != v1alpha1.BlobPhasePending {
		return
	}

	blob = blob.DeepCopy()

	blob.Status.HandlerName = c.handlerName
	blob.Status.Phase = v1alpha1.BlobPhaseRunning
	_, err = c.client.TaskV1alpha1().Blobs().UpdateStatus(ctx, blob, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return
		}
		c.workqueue.AddAfter(name, 5*time.Second)
		klog.Errorf("failed to update blob %s: %v", blob.Name, err)
		return
	}
}
