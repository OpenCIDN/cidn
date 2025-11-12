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
	workqueue    workqueue.TypedDelayingInterface[string]
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
		workqueue:    workqueue.NewTypedDelayingQueue[string](),
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

	key := blob.Name

	c.lastSeenMut.Lock()
	defer c.lastSeenMut.Unlock()
	delete(c.lastSeen, key)
}

func (c *ReleaseBlobController) enqueueBlob(obj interface{}) {
	blob := obj.(*v1alpha1.Blob)
	if blob.Status.HandlerName == "" {
		return
	}

	key := blob.Name

	c.lastSeenMut.Lock()
	c.lastSeen[key] = time.Now()
	c.lastSeenMut.Unlock()
	c.workqueue.Add(key)
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

	next, err := c.chunkHandler(ctx, key)
	if err != nil {
		klog.Errorf("error release blob chunking '%s': %v", key, err)
	}

	if next != 0 {
		c.workqueue.AddAfter(key, next)
	}

	return true
}

func (c *ReleaseBlobController) chunkHandler(ctx context.Context, name string) (time.Duration, error) {
	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	if blob.Status.HandlerName == "" {
		return 0, nil
	}

	c.lastSeenMut.RLock()
	lastSeenTime, ok := c.lastSeen[name]
	c.lastSeenMut.RUnlock()

	if !ok {
		return 0, nil
	}

	switch blob.Status.Phase {
	case v1alpha1.BlobPhaseRunning:
		dur := 8 * time.Minute
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		newBlob := blob.DeepCopy()
		newBlob.Status.Phase = v1alpha1.BlobPhaseUnknown
		klog.Infof("Transitioning blob %s from Running to Unknown phase", name)

		_, err = c.client.TaskV1alpha1().Blobs().UpdateStatus(ctx, newBlob, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update blob %s: %v", name, err)
		}
	case v1alpha1.BlobPhaseUnknown:
		dur := 30 * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		newBlob := blob.DeepCopy()
		newBlob.Status.Phase = v1alpha1.BlobPhasePending
		newBlob.Status.HandlerName = ""
		klog.Infof("Transitioning blob %s from Unknown to Pending phase and clearing handler", name)

		_, err = c.client.TaskV1alpha1().Blobs().UpdateStatus(ctx, newBlob, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update blob %s: %v", name, err)
		}
	case v1alpha1.BlobPhaseFailed:
		ttl, ok := getTTLDuration(blob.ObjectMeta, v1alpha1.ReleaseTTLAnnotation)
		if !ok {
			return 0, nil
		}

		// Use CompletionTime if available, otherwise fall back to lastSeenTime
		var timeSinceCompletion time.Duration
		if blob.Status.CompletionTime != nil {
			timeSinceCompletion = time.Since(blob.Status.CompletionTime.Time)
		} else {
			timeSinceCompletion = time.Since(lastSeenTime)
		}

		if timeSinceCompletion < ttl {
			return ttl - timeSinceCompletion, nil
		}

		klog.Infof("Deleting failed blob %s after %v", name, ttl)
		err = c.client.TaskV1alpha1().Blobs().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return 10 * time.Second, fmt.Errorf("failed to delete blob %s: %v", name, err)
		}
	case v1alpha1.BlobPhaseSucceeded:
		ttl, ok := getTTLDuration(blob.ObjectMeta, v1alpha1.ReleaseTTLAnnotation)
		if !ok {
			return 0, nil
		}

		// Use CompletionTime if available, otherwise fall back to lastSeenTime
		var timeSinceCompletion time.Duration
		if blob.Status.CompletionTime != nil {
			timeSinceCompletion = time.Since(blob.Status.CompletionTime.Time)
		} else {
			timeSinceCompletion = time.Since(lastSeenTime)
		}

		if timeSinceCompletion < ttl {
			return ttl - timeSinceCompletion, nil
		}

		klog.Infof("Deleting succeeded blob %s after %v", name, ttl)
		err = c.client.TaskV1alpha1().Blobs().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return 10 * time.Second, fmt.Errorf("failed to delete blob %s: %v", name, err)
		}
	}
	return 0, nil
}
