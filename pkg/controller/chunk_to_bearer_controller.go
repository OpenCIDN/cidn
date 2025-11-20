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
	"net/http"
	"net/url"
	"strings"
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

type ChunkToBearerController struct {
	handlerName    string
	client         versioned.Interface
	chunkInformer  informers.ChunkInformer
	bearerInformer informers.BearerInformer
	workqueue      workqueue.TypedDelayingInterface[string]
	concurrency    int
}

func NewChunkToBearerController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *ChunkToBearerController {
	c := &ChunkToBearerController{
		handlerName:    handlerName,
		chunkInformer:  sharedInformerFactory.Task().V1alpha1().Chunks(),
		bearerInformer: sharedInformerFactory.Task().V1alpha1().Bearers(),
		client:         client,
		workqueue:      workqueue.NewTypedDelayingQueue[string](),
		concurrency:    5,
	}

	c.chunkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chunk := obj.(*v1alpha1.Chunk)
			bearerName := chunk.Annotations[BearerNameAnnotationKey]
			if bearerName != "" {
				return
			}

			key := chunk.Name
			c.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			chunk := newObj.(*v1alpha1.Chunk)
			bearerName := chunk.Annotations[BearerNameAnnotationKey]
			if bearerName != "" {
				return
			}

			key := chunk.Name
			c.workqueue.Add(key)
		},
	})

	return c
}

func (c *ChunkToBearerController) Start(ctx context.Context) error {
	for i := 0; i < c.concurrency; i++ {
		go c.runWorker(ctx)
	}
	return nil
}

func (c *ChunkToBearerController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ChunkToBearerController) processNextItem(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)

	err := c.handler(ctx, key)
	if err != nil {
		c.workqueue.AddAfter(key, 5*time.Second+time.Duration(rand.Intn(100))*time.Millisecond)
		klog.Errorf("error chunk bearing '%s': %v, requeuing", key, err)
		return true
	}

	return true
}

func (c *ChunkToBearerController) handler(ctx context.Context, name string) error {
	chunk, err := c.chunkInformer.Lister().Get(name)
	if err != nil {
		return err
	}

	if chunk.Status.Phase != v1alpha1.ChunkPhaseFailed {
		return nil
	}

	if !chunk.Status.Retryable {
		return nil
	}

	if chunk.Status.SourceResponse == nil {
		return nil
	}

	if chunk.Status.SourceResponse.StatusCode != http.StatusUnauthorized {
		return nil
	}
	if len(chunk.Status.SourceResponse.Headers) == 0 {
		return nil
	}

	wwwAuthenticate := chunk.Status.SourceResponse.Headers["www-authenticate"]
	if wwwAuthenticate == "" {
		return nil
	}

	if chunk.Spec.BearerName == "" {
		return nil
	}

	_, err = c.bearerInformer.Lister().Get(chunk.Spec.BearerName)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	scheme, params := parseWWWAuthenticate(wwwAuthenticate)
	if scheme != "Bearer" {
		return nil
	}

	realm := params["realm"]
	if realm == "" {
		return nil
	}

	realmURL, err := url.Parse(realm)
	if err != nil {
		klog.Errorf("failed to parse realm URL %q: %v", realm, err)
		return nil
	}

	query := realmURL.Query()
	for k, v := range params {
		if k == "realm" {
			continue
		}
		query.Set(k, v)
	}

	if !query.Has("scope") && chunk.Status.SourceResponse.Headers["docker-distribution-api-version"] != "" &&
		(chunk.Spec.Source.Request.Method == http.MethodHead || chunk.Spec.Source.Request.Method == http.MethodGet) {

		scope, ok := getImage(chunk.Spec.Source.Request.URL)
		if ok {
			query.Set("scope", fmt.Sprintf(`repository:%s:pull`, scope))
		}
	}

	realmURL.RawQuery = query.Encode()

	bearer := &v1alpha1.Bearer{
		ObjectMeta: metav1.ObjectMeta{
			Name: chunk.Spec.BearerName,
			Annotations: map[string]string{
				v1alpha1.ReleaseTTLAnnotation: "24h",
			},
		},
		Spec: v1alpha1.BearerSpec{
			URL:          realmURL.String(),
			Priority:     chunk.Spec.Priority,
			MaximumRetry: chunk.Spec.MaximumRetry - chunk.Status.Retry,
		},
		Status: v1alpha1.BearerStatus{
			Phase: v1alpha1.BearerPhasePending,
		},
	}

	_, err = c.client.TaskV1alpha1().Bearers().Create(ctx, bearer, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	return nil
}

func parseWWWAuthenticate(header string) (string, map[string]string) {
	if header == "" {
		return "", nil
	}

	// Split into scheme and parameters
	parts := strings.SplitN(header, " ", 2)
	if len(parts) < 2 {
		return parts[0], nil
	}

	scheme := parts[0]
	params := make(map[string]string)

	// Parse key=value pairs
	for _, param := range strings.Split(parts[1], ",") {
		param = strings.TrimSpace(param)
		kv := strings.SplitN(param, "=", 2)
		if len(kv) == 2 {
			// Remove quotes from value if present
			value := strings.Trim(kv[1], `"`)
			params[kv[0]] = value
		}
	}

	return scheme, params
}

func getImage(path string) (string, bool) {
	const prefix = "/v2/"
	path = strings.TrimPrefix(path, prefix)
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		return "", false
	}
	return strings.Join(parts[0:len(parts)-2], "/"), true
}
