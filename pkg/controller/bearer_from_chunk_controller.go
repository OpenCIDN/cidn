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
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type BearerFromChunkController struct {
	handlerName    string
	client         versioned.Interface
	bearerInformer informers.BearerInformer
	chunkInformer  informers.ChunkInformer
	workqueue      workqueue.TypedDelayingInterface[string]
	concurrency    int
}

func NewBearerFromChunkController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BearerFromChunkController {
	c := &BearerFromChunkController{
		handlerName:    handlerName,
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		chunkInformer:  sharedInformerFactory.Task().V1alpha1().Chunks(),
		client:         client,
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
		concurrency:    5,
	}

	c.bearerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			bearer := newObj.(*v1alpha1.Bearer)
			key := bearer.Name
			c.workqueue.Add(key)
		},
	})

	c.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chunk, ok := obj.(*v1alpha1.Chunk)
			if !ok {
				return
			}

			bearerName := chunk.Annotations[BearerNameAnnotationKey]
			if bearerName == "" {
				return
			}
			c.workqueue.Add(bearerName)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newChunk := newObj.(*v1alpha1.Chunk)

			bearerName := newChunk.Annotations[BearerNameAnnotationKey]
			if bearerName == "" {
				return
			}
			c.workqueue.Add(bearerName)
		},
		DeleteFunc: func(obj interface{}) {
			chunk, ok := obj.(*v1alpha1.Chunk)
			if !ok {
				return
			}

			bearerName := chunk.Annotations[BearerNameAnnotationKey]
			if bearerName == "" {
				return
			}
			c.workqueue.Add(bearerName)
		},
	})

	return c
}

func (c *BearerFromChunkController) Start(ctx context.Context) error {
	for i := 0; i < c.concurrency; i++ {
		go c.runWorker(ctx)
	}
	return nil
}

func (c *BearerFromChunkController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BearerFromChunkController) processNextItem(ctx context.Context) bool {
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

func (c *BearerFromChunkController) chunkHandler(ctx context.Context, name string) error {
	bearer, err := c.bearerInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if bearer.Status.HandlerName != c.handlerName {
		return nil
	}

	if bearer.Status.Phase == v1alpha1.BearerPhaseSucceeded {
		return nil
	}

	err = c.fromChunk(ctx, bearer)
	if err != nil {
		return fmt.Errorf("failed to update bearer status for bearer %s: %v", bearer.Name, err)
	}

	return nil
}

func (c *BearerFromChunkController) fromChunk(ctx context.Context, bearer *v1alpha1.Bearer) error {
	chunkName := buildBearerChunkName(bearer.Name)
	chunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get chunk: %w", err)
	}

	if chunk.Status.SourceResponse == nil {
		return nil
	}

	switch chunk.Status.Phase {
	case v1alpha1.ChunkPhaseSucceeded:
		bti := v1alpha1.BearerTokenInfo{}
		err = json.Unmarshal(chunk.Status.ResponseBody, &bti)
		if err != nil {
			return fmt.Errorf("failed to unmarshal chunk response body: %v", err)
		}

		_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Bearers(), bearer, func(b *v1alpha1.Bearer) *v1alpha1.Bearer {
			b.Status.TokenInfo = &bti
			b.Status.Phase = v1alpha1.BearerPhaseSucceeded
			return b
		})
		if err != nil {
			return fmt.Errorf("failed to update bearer status: %v", err)
		}

		err = c.client.TaskV1alpha1().Chunks().Delete(ctx, chunkName, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to update bearer status: %v", err)
			}
		}
	case v1alpha1.ChunkPhaseFailed:
		_, err = utils.UpdateResourceStatusWithRetry(ctx, c.client.TaskV1alpha1().Bearers(), bearer, func(b *v1alpha1.Bearer) *v1alpha1.Bearer {
			retryable := chunk.Status.Retryable && chunk.Status.Retry < chunk.Spec.MaximumRetry
			if retryable {
				b.Status.Phase = v1alpha1.BearerPhaseRunning
			} else {
				b.Status.Phase = v1alpha1.BearerPhaseFailed
				b.Status.Conditions = v1alpha1.AppendConditions(b.Status.Conditions, chunk.Status.Conditions...)
			}
			return b
		})
		if err != nil {
			return fmt.Errorf("failed to update bearer status: %v", err)
		}
	}
	return nil
}
