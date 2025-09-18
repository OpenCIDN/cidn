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

	if bearer.Status.Phase != v1alpha1.BearerPhaseRunning &&
		bearer.Status.Phase != v1alpha1.BearerPhaseUnknown &&
		bearer.Status.Phase != v1alpha1.BearerPhaseFailed {
		return
	}

	key := bearer.Name

	c.lastSeenMut.Lock()
	c.lastSeen[key] = time.Now()
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

	next, err := c.chunkHandler(ctx, key)
	if err != nil {
		klog.Errorf("error release bearer chunking '%s': %v", key, err)
	}

	if next != 0 {
		c.workqueue.AddAfter(key, next)
	}

	return true
}

func (c *ReleaseBearerController) chunkHandler(ctx context.Context, name string) (time.Duration, error) {
	bearer, err := c.bearerInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	if bearer.Status.HandlerName == "" {
		return 0, nil
	}

	c.lastSeenMut.RLock()
	lastSeenTime, ok := c.lastSeen[name]
	c.lastSeenMut.RUnlock()

	if !ok {
		return 0, nil
	}

	switch bearer.Status.Phase {
	case v1alpha1.BearerPhaseSucceeded:
		issuedAt := bearer.Status.TokenInfo.IssuedAt.Time
		expiresIn := bearer.Status.TokenInfo.ExpiresIn

		since := time.Since(issuedAt)
		expires := time.Duration(expiresIn) * time.Second

		if since < expires+expires/4 {
			return expires - since + expires/4, nil
		}

		err = c.client.TaskV1alpha1().Bearers().Delete(ctx, bearer.Name, metav1.DeleteOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update bearer %s: %v", name, err)
		}
	case v1alpha1.BearerPhaseRunning:
		dur := 40 * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		newBearer := bearer.DeepCopy()
		newBearer.Status.Phase = v1alpha1.BearerPhaseUnknown
		klog.Infof("Transitioning bearer %s from Running to Unknown phase", name)

		_, err = c.client.TaskV1alpha1().Bearers().UpdateStatus(ctx, newBearer, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update bearer %s: %v", name, err)
		}
	case v1alpha1.BearerPhaseUnknown:
		dur := 20 * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		newBearer := bearer.DeepCopy()
		newBearer.Status.Phase = v1alpha1.BearerPhasePending
		newBearer.Status.HandlerName = ""
		klog.Infof("Transitioning bearer %s from Unknown to Pending phase and clearing handler", name)

		_, err = c.client.TaskV1alpha1().Bearers().UpdateStatus(ctx, newBearer, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update bearer %s: %v", name, err)
		}
	case v1alpha1.BearerPhaseFailed:
		dur := time.Duration(math.Pow(2, float64(bearer.Status.Retry))) * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		if bearer.Status.Retry < bearer.Spec.MaximumRetry {
			if _, ok := v1alpha1.GetCondition(bearer.Status.Conditions, v1alpha1.ConditionTypeRetryable); ok {
				newBearer := bearer.DeepCopy()
				newBearer.Status.Phase = v1alpha1.BearerPhasePending
				newBearer.Status.Conditions = nil
				newBearer.Status.Retry++
				newBearer.Status.HandlerName = ""
				klog.Infof("Transitioning bearer %s from Failed to Pending phase and clearing handler", name)

				_, err = c.client.TaskV1alpha1().Bearers().UpdateStatus(ctx, newBearer, metav1.UpdateOptions{})
				if err != nil {
					return 10 * time.Second, fmt.Errorf("failed to update bearer %s: %v", name, err)
				}
			}
		}
	}
	return 0, nil
}
