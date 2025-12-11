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

type ReleaseBearerController struct {
	handlerName    string
	client         versioned.Interface
	bearerInformer informers.BearerInformer
	workqueue      workqueue.TypedDelayingInterface[string]
	lastSeen       map[string]time.Time
	lastSeenMut    sync.RWMutex
}

func NewReleaseBearerController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ReleaseBearerController {
	c := &ReleaseBearerController{
		handlerName:    handlerName,
		client:         client,
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
		lastSeen:       map[string]time.Time{},
	}
	c.bearerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueBearer(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueBearer(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.cleanupBearer(obj)
		},
	})

	return c
}

func (c *ReleaseBearerController) Start(ctx context.Context) error {
	go c.runWorker(ctx)
	return nil
}

func (c *ReleaseBearerController) cleanupBearer(obj interface{}) {
	bearer, ok := obj.(*v1alpha1.Bearer)
	if !ok {
		return
	}

	key := bearer.Name

	c.lastSeenMut.Lock()
	defer c.lastSeenMut.Unlock()
	delete(c.lastSeen, key)
}

func (c *ReleaseBearerController) enqueueBearer(obj interface{}) {
	bearer := obj.(*v1alpha1.Bearer)
	if bearer.Status.HandlerName == "" {
		return
	}

	key := bearer.Name

	now := time.Now()
	c.lastSeenMut.Lock()
	c.lastSeen[key] = now
	c.lastSeenMut.Unlock()
	c.workqueue.Add(key)
}

func (c *ReleaseBearerController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ReleaseBearerController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	c.handler(ctx, key)

	return true
}

func (c *ReleaseBearerController) handler(ctx context.Context, name string) {
	c.lastSeenMut.RLock()
	lastSeenTime, ok := c.lastSeen[name]
	c.lastSeenMut.RUnlock()
	if !ok {
		return
	}

	bearer, err := c.bearerInformer.Lister().Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to get bearer '%s': %v", name, err)
			return
		}
		bearer, err = c.client.TaskV1alpha1().Bearers().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to get bearer '%s' from API server: %v", name, err)
			return
		}
	}

	if bearer.Status.HandlerName == "" {
		return
	}

	switch bearer.Status.Phase {
	case v1alpha1.BearerPhaseRunning:
		dur := 3 * time.Minute
		sub := time.Since(lastSeenTime)
		if sub < dur {
			c.workqueue.AddAfter(name, max(time.Second, dur-sub))
			return
		}

		bearer := bearer.DeepCopy()
		bearer.Status.Phase = v1alpha1.BearerPhaseUnknown

		_, err = c.client.TaskV1alpha1().Bearers().UpdateStatus(ctx, bearer, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsConflict(err) {
			c.workqueue.AddAfter(name, 10*time.Second)
			klog.Errorf("failed to update bearer %s: %v", name, err)
			return
		}
	case v1alpha1.BearerPhaseUnknown:
		dur := 30 * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			c.workqueue.AddAfter(name, max(time.Second, dur-sub))
			return
		}
		err = c.client.TaskV1alpha1().Bearers().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			c.workqueue.AddAfter(name, 10*time.Second)
			klog.Errorf("failed to delete bearer %s: %v", name, err)
			return
		}
	case v1alpha1.BearerPhaseFailed:
		ttl, ok := getTTLDuration(bearer.ObjectMeta, v1alpha1.ReleaseTTLAnnotation)
		if !ok {
			return
		}

		sub := time.Since(lastSeenTime)
		if sub < ttl {
			c.workqueue.AddAfter(name, max(time.Second, ttl-sub))
			return
		}

		err = c.client.TaskV1alpha1().Bearers().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			c.workqueue.AddAfter(name, 10*time.Second)
			klog.Errorf("failed to delete bearer %s: %v", name, err)
			return
		}
	case v1alpha1.BearerPhaseSucceeded:
		expiresIn := bearer.Status.TokenInfo.ExpiresIn
		issuedAt := bearer.Status.TokenInfo.IssuedAt.Time
		if expiresIn != 0 && !issuedAt.IsZero() {
			expires := time.Duration(expiresIn) * time.Second

			expirationTime := issuedAt.Add(expires)
			if !expirationTime.After(time.Now()) {
				err = c.client.TaskV1alpha1().Bearers().Delete(ctx, bearer.Name, metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					c.workqueue.AddAfter(name, 10*time.Second)
					klog.Errorf("failed to delete bearer %s: %v", name, err)
					return
				}
				return
			}

			c.workqueue.AddAfter(name, time.Until(expirationTime)+10*time.Second)
			return
		}

		ttl, ok := getTTLDuration(bearer.ObjectMeta, v1alpha1.ReleaseTTLAnnotation)
		if !ok {
			return
		}
		sub := time.Since(lastSeenTime)
		if sub < ttl {
			c.workqueue.AddAfter(name, max(time.Second, ttl-sub))
			return
		}

		err = c.client.TaskV1alpha1().Bearers().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			c.workqueue.AddAfter(name, 10*time.Second)
			klog.Errorf("failed to delete bearer %s: %v", name, err)
			return
		}
	}
}
