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
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type ChunkFromBearerController struct {
	handlerName    string
	client         versioned.Interface
	blobInformer   informers.BlobInformer
	chunkInformer  informers.ChunkInformer
	bearerInformer informers.BearerInformer
	workqueue      workqueue.TypedDelayingInterface[string]
	concurrency    int
}

func NewChunkFromBearerController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ChunkFromBearerController {
	c := &ChunkFromBearerController{
		handlerName:    handlerName,
		blobInformer:   sharedInformerFactory.Task().V1alpha1().Blobs(),
		chunkInformer:  sharedInformerFactory.Task().V1alpha1().Chunks(),
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		client:         client,
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
		concurrency:    5,
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
		DeleteFunc: func(obj interface{}) {
			bearer, ok := obj.(*v1alpha1.Bearer)
			if !ok {
				return
			}
			key := bearer.Name
			c.workqueue.Done(key)
		},
	})

	return c
}

func (c *ChunkFromBearerController) Start(ctx context.Context) error {
	for i := 0; i < c.concurrency; i++ {
		go c.runWorker(ctx)
	}
	return nil
}

func (c *ChunkFromBearerController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ChunkFromBearerController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.handler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error blob chunking '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *ChunkFromBearerController) handler(ctx context.Context, name string) error {
	chunkList, err := c.client.TaskV1alpha1().Chunks().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, chunk := range chunkList.Items {
		if chunk.Spec.BearerName != name {
			continue
		}
		if chunk.Status.Phase != v1alpha1.ChunkPhaseFailed {
			continue
		}

		if !chunk.Status.Retryable {
			continue
		}

		_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Chunks(), &chunk, func(ch *v1alpha1.Chunk) *v1alpha1.Chunk {
			ch.Status.HandlerName = ""
			ch.Status.Phase = v1alpha1.ChunkPhasePending
			ch.Status.Progress = 0
			ch.Status.Conditions = nil
			return ch
		})
		if err != nil {
			klog.Errorf("error resetting chunk '%s' status: %v", chunk.Name, err)
		}
	}

	return nil
}
