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

type ReleaseSyncController struct {
	handlerName  string
	client       versioned.Interface
	syncInformer informers.SyncInformer
	workqueue    workqueue.TypedDelayingInterface[string]
	lastSeen     map[string]time.Time
	lastSeenMut  sync.RWMutex
}

func NewReleaseSyncController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ReleaseSyncController {
	c := &ReleaseSyncController{
		handlerName:  handlerName,
		client:       client,
		syncInformer: sharedInformerFactory.Task().V1alpha1().Syncs(),
		workqueue:    workqueue.NewTypedDelayingQueue[string](),
		lastSeen:     map[string]time.Time{},
	}
	c.syncInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueSync(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueSync(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.cleanupSync(obj)
		},
	})

	return c
}

func (c *ReleaseSyncController) Start(ctx context.Context) error {
	go c.runWorker(ctx)
	return nil
}

func (c *ReleaseSyncController) cleanupSync(obj interface{}) {
	blob, ok := obj.(*v1alpha1.Sync)
	if !ok {
		return
	}

	key := blob.Name

	c.lastSeenMut.Lock()
	defer c.lastSeenMut.Unlock()
	delete(c.lastSeen, key)
}

func (c *ReleaseSyncController) enqueueSync(obj interface{}) {
	sync := obj.(*v1alpha1.Sync)
	if sync.Spec.HandlerName == "" {
		return
	}

	if sync.Status.Phase != v1alpha1.SyncPhaseRunning &&
		sync.Status.Phase != v1alpha1.SyncPhaseUnknown &&
		sync.Status.Phase != v1alpha1.SyncPhaseFailed {
		return
	}

	key := sync.Name

	c.lastSeenMut.Lock()
	c.lastSeen[key] = time.Now()
	c.lastSeenMut.Unlock()
	c.workqueue.Add(key)
}

func (c *ReleaseSyncController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ReleaseSyncController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	next, err := c.syncHandler(ctx, key)
	if err != nil {
		klog.Errorf("error release sync syncing '%s': %v", key, err)
	}

	if next != 0 {
		c.workqueue.AddAfter(key, next)
	}

	return true
}

func (c *ReleaseSyncController) syncHandler(ctx context.Context, name string) (time.Duration, error) {
	sync, err := c.syncInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	if sync.Spec.HandlerName == "" {
		return 0, nil
	}

	c.lastSeenMut.RLock()
	lastSeenTime, ok := c.lastSeen[name]
	c.lastSeenMut.RUnlock()

	if !ok {
		return 0, nil
	}
	switch sync.Status.Phase {
	case v1alpha1.SyncPhaseRunning:
		if time.Since(lastSeenTime) < 30*time.Second {
			return 30*time.Second - time.Since(lastSeenTime), nil
		}

		newSync := sync.DeepCopy()
		newSync.Status.Phase = v1alpha1.SyncPhaseUnknown
		klog.Infof("Transitioning sync %s from Running to Unknown phase", name)

		_, err = c.client.TaskV1alpha1().Syncs().Update(ctx, newSync, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update sync %s: %v", name, err)
		}
	case v1alpha1.SyncPhaseUnknown:
		if time.Since(lastSeenTime) < 20*time.Second {
			return 20*time.Second - time.Since(lastSeenTime), nil
		}

		newSync := sync.DeepCopy()
		newSync.Status.Phase = v1alpha1.SyncPhasePending
		newSync.Spec.HandlerName = ""
		klog.Infof("Transitioning sync %s from Unknown to Pending phase and clearing handler", name)

		_, err = c.client.TaskV1alpha1().Syncs().Update(ctx, newSync, metav1.UpdateOptions{})
		if err != nil {
			return 10 * time.Second, fmt.Errorf("failed to update sync %s: %v", name, err)
		}
	case v1alpha1.SyncPhaseFailed:
		dur := time.Duration(math.Pow(2, float64(sync.Status.RetryCount))) * time.Second
		sub := time.Since(lastSeenTime)
		if sub < dur {
			return dur - sub, nil
		}

		if sync.Status.RetryCount < sync.Spec.RetryCount {
			if _, ok := v1alpha1.GetCondition(sync.Status.Conditions, v1alpha1.ConditionTypeRetryable); ok {
				newSync := sync.DeepCopy()
				newSync.Status.Phase = v1alpha1.SyncPhasePending
				newSync.Status.Conditions = nil
				newSync.Status.RetryCount++
				newSync.Spec.HandlerName = ""
				klog.Infof("Transitioning sync %s from Failed to Pending phase and clearing handler", name)

				_, err = c.client.TaskV1alpha1().Syncs().Update(ctx, newSync, metav1.UpdateOptions{})
				if err != nil {
					return 10 * time.Second, fmt.Errorf("failed to update sync %s: %v", name, err)
				}
			}
		}
	}
	return 0, nil
}
