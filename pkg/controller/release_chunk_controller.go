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

type ReleaseChunkController struct {
	handlerName   string
	client        versioned.Interface
	chunkInformer informers.ChunkInformer
	workqueue     workqueue.TypedDelayingInterface[string]
	lastSeen      map[string]time.Time
	lastSeenMut   sync.RWMutex
}

func NewReleaseChunkController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ReleaseChunkController {
	c := &ReleaseChunkController{
		handlerName:   handlerName,
		client:        client,
		chunkInformer: sharedInformerFactory.Task().V1alpha1().Chunks(),
		workqueue:     workqueue.NewTypedDelayingQueue[string](),
		lastSeen:      map[string]time.Time{},
	}
	c.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueChunk(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueChunk(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.cleanupChunk(obj)
		},
	})

	return c
}

func (c *ReleaseChunkController) Start(ctx context.Context) error {
	go c.runWorker(ctx)
	return nil
}

func (c *ReleaseChunkController) cleanupChunk(obj interface{}) {
	blob, ok := obj.(*v1alpha1.Chunk)
	if !ok {
		return
	}

	key := blob.Name

	c.lastSeenMut.Lock()
	defer c.lastSeenMut.Unlock()
	delete(c.lastSeen, key)
}

func (c *ReleaseChunkController) enqueueChunk(obj interface{}) {
	chunk := obj.(*v1alpha1.Chunk)
	if chunk.Status.HandlerName == "" {
		return
	}

	if chunk.Status.Phase != v1alpha1.ChunkPhaseRunning &&
		chunk.Status.Phase != v1alpha1.ChunkPhaseUnknown &&
		chunk.Status.Phase != v1alpha1.ChunkPhaseFailed &&
		chunk.Status.Phase != v1alpha1.ChunkPhaseSucceeded {
		return
	}

	key := chunk.Name

	c.lastSeenMut.Lock()
	c.lastSeen[key] = time.Now()
	c.lastSeenMut.Unlock()
	c.workqueue.Add(key)
}

func (c *ReleaseChunkController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ReleaseChunkController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	next, err := c.chunkHandler(ctx, key)
	if err != nil {
		klog.Errorf("error release chunk chunking '%s': %v", key, err)
	}

	if next != 0 {
		c.workqueue.AddAfter(key, next)
	}

	return true
}

func (c *ReleaseChunkController) chunkHandler(ctx context.Context, name string) (time.Duration, error) {
	chunk, err := c.chunkInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	if chunk.Status.HandlerName == "" {
		return 0, nil
	}

	c.lastSeenMut.RLock()
	lastSeenTime, ok := c.lastSeen[name]
	c.lastSeenMut.RUnlock()

	if !ok {
		return 0, nil
	}
	switch chunk.Status.Phase {
	case v1alpha1.ChunkPhaseRunning:
		dur := 120 * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		newChunk := chunk.DeepCopy()
		newChunk.Status.Phase = v1alpha1.ChunkPhaseUnknown
		klog.Infof("Transitioning chunk %s from Running to Unknown phase", name)

		_, err = c.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, newChunk, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update chunk %s: %v", name, err)
		}
	case v1alpha1.ChunkPhaseUnknown:
		dur := 30 * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		newChunk := chunk.DeepCopy()
		newChunk.Status.Phase = v1alpha1.ChunkPhasePending
		newChunk.Status.HandlerName = ""
		klog.Infof("Transitioning chunk %s from Unknown to Pending phase and clearing handler", name)

		_, err = c.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, newChunk, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update chunk %s: %v", name, err)
		}
	case v1alpha1.ChunkPhaseFailed:
		if chunk.Status.Retryable {
			dur := time.Duration(math.Pow(2, float64(chunk.Status.Retry))) * time.Second
			sub := time.Since(lastSeenTime)
			if sub < dur {
				return dur - sub, nil
			}

			newChunk := chunk.DeepCopy()
			newChunk.Status.Phase = v1alpha1.ChunkPhasePending
			newChunk.Status.Conditions = nil
			newChunk.Status.Retryable = false
			newChunk.Status.Retry++
			newChunk.Status.HandlerName = ""
			klog.Infof("Transitioning chunk %s from Failed to Pending phase and clearing handler", name)

			_, err = c.client.TaskV1alpha1().Chunks().UpdateStatus(ctx, newChunk, metav1.UpdateOptions{})
			if err != nil {
				return 10 * time.Second, fmt.Errorf("failed to update chunk %s: %v", name, err)
			}
		} else {
			// If the chunk has a TTL annotation and is not retryable, delete it after the TTL
			ttl, ok := getTTLDuration(chunk.ObjectMeta, v1alpha1.ReleaseTTLAnnotation)
			if !ok {
				return 0, nil
			}

			sub := time.Since(lastSeenTime)
			if sub < ttl {
				return ttl - sub, nil
			}

			klog.Infof("Deleting failed chunk %s after %v", name, ttl)
			err = c.client.TaskV1alpha1().Chunks().Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return 10 * time.Second, fmt.Errorf("failed to delete chunk %s: %v", name, err)
			}
		}
	case v1alpha1.ChunkPhaseSucceeded:
		// Only delete succeeded chunks if TTL annotation is set
		ttl, ok := getTTLDuration(chunk.ObjectMeta, v1alpha1.ReleaseTTLAnnotation)
		if !ok {
			return 0, nil
		}

		sub := time.Since(lastSeenTime)
		if sub < ttl {
			return ttl - sub, nil
		}

		klog.Infof("Deleting succeeded chunk %s after %v", name, ttl)
		err = c.client.TaskV1alpha1().Chunks().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return 10 * time.Second, fmt.Errorf("failed to delete chunk %s: %v", name, err)
		}
	}
	return 0, nil
}
