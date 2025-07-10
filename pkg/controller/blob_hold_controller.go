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
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	})

	return c
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

	err := c.syncHandler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error blob syncing '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *BlobHoldController) syncHandler(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("invalid resource key: %s", key)
		return nil
	}

	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		return err
	}

	if blob.Spec.HandlerName != "" {
		return nil
	}

	if blob.Spec.Total == 0 {
		return nil
	}
	if blob.Status.Phase != v1alpha1.BlobPhasePending {
		return nil
	}

	blob.Spec.HandlerName = c.handlerName
	blob.Status.Phase = v1alpha1.BlobPhaseRunning
	_, err = c.client.TaskV1alpha1().Blobs().Update(ctx, blob, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update blob %s: %v", blob.Name, err)
	}

	return nil
}
