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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type ReleaseBlobController struct {
	handlerName  string
	client       versioned.Interface
	blobInformer informers.BlobInformer
	workqueue    workqueue.TypedRateLimitingInterface[string]
	lastSeen     map[string]time.Time
	lastSeenMut  sync.RWMutex
}

func NewReleaseBlobController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ReleaseBlobController {
	c := &ReleaseBlobController{
		handlerName:  handlerName,
		client:       client,
		blobInformer: sharedInformerFactory.Task().V1alpha1().Blobs(),
		workqueue:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		lastSeen:     map[string]time.Time{},
	}
	c.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueBlob(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueBlob(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.cleanupBlob(obj)
		},
	})

	return c
}

func (c *ReleaseBlobController) Start(ctx context.Context) error {
	go c.runWorker(ctx)
	return nil
}

func (c *ReleaseBlobController) cleanupBlob(obj interface{}) {
	blob, ok := obj.(*v1alpha1.Blob)
	if !ok {
		return
	}

	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(blob)
	if err != nil {
		klog.Errorf("couldn't get key for blob %+v: %v", blob, err)
		return
	}

	c.lastSeenMut.Lock()
	defer c.lastSeenMut.Unlock()
	delete(c.lastSeen, key)
}

func (c *ReleaseBlobController) enqueueBlob(obj interface{}) {
	blob := obj.(*v1alpha1.Blob)
	if blob.Status.Phase == v1alpha1.BlobPhaseRunning && blob.Spec.HandlerName != "" {
		key, err := cache.MetaNamespaceKeyFunc(blob)
		if err != nil {
			klog.Errorf("couldn't get key for blob %+v: %v", blob, err)
			return
		}

		c.lastSeenMut.Lock()
		c.lastSeen[key] = time.Now()
		c.lastSeenMut.Unlock()

		c.workqueue.AddAfter(key, 10*time.Second)
	}
}

func (c *ReleaseBlobController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ReleaseBlobController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.syncHandler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 10*time.Second)
		klog.Errorf("error syncing '%s': %v, requeuing", key, err)
		return true
	}

	c.workqueue.Forget(key)
	return true
}

func (c *ReleaseBlobController) syncHandler(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("invalid resource key: %s", key)
		return nil
	}

	klog.Infof("processing blob %q", name)

	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if blob.Status.Phase != v1alpha1.BlobPhaseRunning || blob.Spec.HandlerName == "" {
		return nil
	}

	c.lastSeenMut.RLock()
	lastSeenTime, ok := c.lastSeen[key]
	c.lastSeenMut.RUnlock()

	if !ok {
		return nil
	}

	if time.Since(lastSeenTime) < 10*time.Second {
		return fmt.Errorf("not enough time: %s", key)
	}

	// Reset to pending and clear handler name
	newBlob := blob.DeepCopy()
	newBlob.Status.Phase = v1alpha1.BlobPhasePending
	newBlob.Spec.HandlerName = ""

	_, err = c.client.TaskV1alpha1().Blobs().Update(ctx, newBlob, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update blob %s: %v", name, err)
	}

	return nil
}
