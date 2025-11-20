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
	"net/http"
	"net/url"
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

type BearerToChunkController struct {
	handlerName    string
	users          map[string]*url.Userinfo
	client         versioned.Interface
	bearerInformer informers.BearerInformer
	chunkInformer  informers.ChunkInformer
	workqueue      workqueue.TypedDelayingInterface[string]
	concurrency    int
}

func NewBearerToChunkController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
	users []utils.UserValue,
) *BearerToChunkController {
	c := &BearerToChunkController{
		handlerName:    handlerName,
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		chunkInformer:  sharedInformerFactory.Task().V1alpha1().Chunks(),
		client:         client,
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
		users:          map[string]*url.Userinfo{},
		concurrency:    5,
	}

	for _, u := range users {
		up := url.UserPassword(u.Name, u.Password)
		for _, g := range u.Groups {
			if g == "" {
				continue
			}
			c.users[g] = up
		}
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
		DeleteFunc: func(obj interface{}) {
			bearer, ok := obj.(*v1alpha1.Bearer)
			if !ok {
				return
			}

			c.cleanupBearer(bearer)
		},
	})

	return c
}

func (c *BearerToChunkController) Start(ctx context.Context) error {
	for i := 0; i < c.concurrency; i++ {
		go c.runWorker(ctx)
	}
	return nil
}

func (c *BearerToChunkController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *BearerToChunkController) cleanupBearer(bearer *v1alpha1.Bearer) {
	err := c.client.TaskV1alpha1().Chunks().DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labels.Set{
			BearerUIDLabelKey: string(bearer.UID),
		}.String(),
	})
	if err != nil {
		klog.Errorf("failed to delete chunks for bearer %s: %v", bearer.Name, err)
	}
}

func (c *BearerToChunkController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	c.handler(ctx, key)

	return true
}

func (c *BearerToChunkController) handler(ctx context.Context, name string) {
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

	if bearer.Status.HandlerName != c.handlerName {
		return
	}

	switch bearer.Status.Phase {
	case v1alpha1.BearerPhaseRunning, v1alpha1.BearerPhaseUnknown:
		err := c.toChunk(ctx, bearer)
		if err != nil {
			c.workqueue.AddAfter(name, 5*time.Second)
			klog.Errorf("failed to create chunk for bearer %s: %v", bearer.Name, err)
			return
		}

	case v1alpha1.BearerPhaseSucceeded:
		c.cleanupBearer(bearer)
	}
}

func buildBearerChunkName(bearerName string) string {
	return fmt.Sprintf("bearer:%s", bearerName)
}

func (c *BearerToChunkController) toChunk(ctx context.Context, bearer *v1alpha1.Bearer) error {
	chunkName := buildBearerChunkName(bearer.Name)
	existingChunk, err := c.chunkInformer.Lister().Get(chunkName)
	if err == nil && existingChunk != nil {
		return nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing chunk: %v", err)
	}

	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name: chunkName,
			Labels: map[string]string{
				BearerUIDLabelKey: string(bearer.UID),
			},
			Annotations: map[string]string{
				BearerNameAnnotationKey: bearer.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       v1alpha1.BearerKind,
					Name:       bearer.Name,
					UID:        bearer.UID,
				},
			},
		},
		Spec: v1alpha1.ChunkSpec{
			Priority:           bearer.Spec.Priority + 1,
			MaximumRetry:       bearer.Spec.MaximumRetry,
			InlineResponseBody: true,
		},
		Status: v1alpha1.ChunkStatus{
			Phase: v1alpha1.ChunkPhasePending,
		},
	}

	header := map[string]string{}

	src := bearer.Spec.URL

	addBasicAuth(&src, header, c.users)

	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request: v1alpha1.ChunkHTTPRequest{
			Method:  http.MethodGet,
			URL:     src,
			Headers: header,
		},
		Response: v1alpha1.ChunkHTTPResponse{
			StatusCode: http.StatusOK,
		},
	}

	_, err = c.client.TaskV1alpha1().Chunks().Create(ctx, chunk, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

func addBasicAuth(uri *string, header map[string]string, users map[string]*url.Userinfo) error {
	u, err := url.Parse(*uri)
	if err != nil {
		return err
	}

	if u.User == nil && header["Authorization"] == "" {
		user, ok := users[u.Host]
		if ok {
			u.User = user
		}
	}

	if u.User != nil {
		header["Authorization"] = utils.FormathBasicAuth(u.User)
		u.User = nil
		*uri = u.String()
	}

	return nil
}
