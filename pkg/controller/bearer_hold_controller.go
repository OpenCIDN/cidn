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

type BearerHoldController struct {
	handlerName    string
	client         versioned.Interface
	bearerInformer informers.BearerInformer
	workqueue      workqueue.TypedDelayingInterface[string]
}

func NewBearerHoldController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BearerHoldController {
	c := &BearerHoldController{
		handlerName:    handlerName,
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		client:         client,
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
	}

	c.bearerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			bearer := obj.(*v1alpha1.Bearer)
			key := bearer.Name
			c.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			bearer := newObj.(*v1alpha1.Bearer)
			key := bearer.Name
			c.workqueue.Add(key)
		},
	})

	return c
}

func (c *BearerHoldController) ReleaseAll(ctx context.Context) error {
	bearers, err := c.bearerInformer.Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list bearers: %w", err)
	}

	var wg sync.WaitGroup

	for _, bearer := range bearers {
		if bearer.Status.HandlerName != c.handlerName {
			continue
		}

		if bearer.Status.Phase != v1alpha1.BearerPhasePending && bearer.Status.Phase != v1alpha1.BearerPhaseRunning && bearer.Status.Phase != v1alpha1.BearerPhaseUnknown {
			continue
		}

		wg.Add(1)
		go func(b *v1alpha1.Bearer) {
			defer wg.Done()

			_, err := utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Bearers(), b, func(bearer *v1alpha1.Bearer) *v1alpha1.Bearer {
				bearer.Status.HandlerName = ""
				bearer.Status.Phase = v1alpha1.BearerPhasePending
				bearer.Status.Conditions = nil
				return bearer
			})
			if err != nil {
				klog.Errorf("failed to release bearer %s: %v", b.Name, err)
			}
		}(bearer)
	}

	wg.Wait()

	return nil
}

func (c *BearerHoldController) Shutdown(ctx context.Context) error {
	return c.ReleaseAll(ctx)
}

func (c *BearerHoldController) Start(ctx context.Context) error {
	go c.runWorker(ctx)
	return nil
}

func (c *BearerHoldController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BearerHoldController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.chunkHandler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error bearer chunking '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *BearerHoldController) chunkHandler(ctx context.Context, name string) error {
	bearer, err := c.bearerInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if bearer.Status.HandlerName != "" {
		return nil
	}

	if bearer.Status.Phase != v1alpha1.BearerPhasePending {
		return nil
	}

	bearer.Status.HandlerName = c.handlerName
	bearer.Status.Phase = v1alpha1.BearerPhaseRunning
	_, err = c.client.TaskV1alpha1().Bearers().UpdateStatus(ctx, bearer, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("failed to update bearer %s: %v", bearer.Name, err)
	}

	return nil
}
