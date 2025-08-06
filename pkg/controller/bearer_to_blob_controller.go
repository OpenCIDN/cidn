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
	"math/rand"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type BearerToBlobController struct {
	handlerName    string
	client         versioned.Interface
	blobInformer   informers.BlobInformer
	chunkInformer  informers.ChunkInformer
	bearerInformer informers.BearerInformer
	workqueue      workqueue.TypedDelayingInterface[string]
}

func NewBearerToBlobController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BearerToBlobController {
	c := &BearerToBlobController{
		handlerName:    handlerName,
		blobInformer:   sharedInformerFactory.Task().V1alpha1().Blobs(),
		chunkInformer:  sharedInformerFactory.Task().V1alpha1().Chunks(),
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		client:         client,
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
	}

	c.bearerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			bearer := obj.(*v1alpha1.Bearer)
			key := bearer.Name

			if bearer.Status.Phase != v1alpha1.BearerPhaseSucceeded {
				return
			}

			c.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			bearer := newObj.(*v1alpha1.Bearer)
			key := bearer.Name

			if bearer.Status.Phase != v1alpha1.BearerPhaseSucceeded {
				return
			}

			c.workqueue.Add(key)
		},
	})

	return c
}

func (c *BearerToBlobController) Start(ctx context.Context) error {

	go c.runWorker(ctx)
	return nil
}

func (c *BearerToBlobController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BearerToBlobController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.blobHandler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error blob chunking '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *BearerToBlobController) blobHandler(ctx context.Context, name string) error {
	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if blob.Status.Phase != v1alpha1.BlobPhaseFailed {
		return nil
	}

	chunkList, err := c.client.TaskV1alpha1().Chunks().List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set{
			BlobUIDLabelKey: string(blob.UID),
		}.String(),
	})
	if err != nil {
		return err
	}

	for _, chunk := range chunkList.Items {
		err := c.client.TaskV1alpha1().Chunks().Delete(context.Background(), chunk.Name, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	blob.Status = v1alpha1.BlobStatus{
		Phase: v1alpha1.BlobPhasePending,
		Total: blob.Status.Total,
	}

	_, err = c.client.TaskV1alpha1().Blobs().UpdateStatus(ctx, blob, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}
