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

package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/versions"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// HeadRunner executes HEAD requests for blob tasks
type HeadRunner struct {
	handlerName  string
	client       versioned.Interface
	blobInformer informers.BlobInformer
	httpClient   *http.Client
	workqueue    workqueue.TypedRateLimitingInterface[string]
}

// NewHeadRunner creates a new HeadRunner instance
func NewHeadRunner(
	handlerName string,
	clientset versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *HeadRunner {
	r := &HeadRunner{
		handlerName:  handlerName,
		client:       clientset,
		blobInformer: sharedInformerFactory.Task().V1alpha1().Blobs(),
		httpClient:   http.DefaultClient,
		workqueue:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
	r.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				klog.Errorf("Couldn't get key for object %+v: %v", obj, err)
				return
			}
			r.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err != nil {
				klog.Errorf("Couldn't get key for object %+v: %v", newObj, err)
				return
			}
			r.workqueue.Add(key)
		},
		DeleteFunc: func(obj interface{}) {
			// No action needed on delete
		},
	})

	return r
}

// Start starts the head runner
func (r *HeadRunner) Start(ctx context.Context) error {

	go r.runWorker(ctx)

	return nil
}

func (r *HeadRunner) runWorker(ctx context.Context) {
	for r.processNextItem(ctx) {
	}
}

func (r *HeadRunner) processNextItem(ctx context.Context) bool {
	key, quit := r.workqueue.Get()
	if quit {
		return false
	}
	defer r.workqueue.Done(key)

	err := r.processBlob(ctx, key)
	if err == nil {
		r.workqueue.Forget(key)
		return true
	}

	klog.Errorf("Error processing blob %q: %v", key, err)
	r.workqueue.AddRateLimited(key)
	return true
}

func (r *HeadRunner) processBlob(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid resource key: %s", key)
	}

	blob, err := r.blobInformer.Lister().Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if blob.Spec.HandlerName == "" {
		if blob.Spec.Total != 0 {
			return nil
		}

		blobCopy := blob.DeepCopy()
		blobCopy.Spec.HandlerName = r.handlerName
		_, err = r.client.TaskV1alpha1().Blobs().Update(ctx, blobCopy, metav1.UpdateOptions{})
		return err
	}

	if blob.Spec.HandlerName != r.handlerName {
		return nil
	}

	fi, err := httpStat(blob.Spec.Source, r.httpClient, nil)
	if err != nil {
		blobCopy := blob.DeepCopy()
		blobCopy.Status.Phase = v1alpha1.BlobPhaseFailed
		hasHTTPStatFailed := false
		for _, condition := range blobCopy.Status.Conditions {
			if condition.Type == "HTTPHead" {
				hasHTTPStatFailed = true
				break
			}
		}
		if !hasHTTPStatFailed {
			blobCopy.Status.Conditions = append(blobCopy.Status.Conditions, v1alpha1.Condition{
				Type:               "HTTPHead",
				Status:             v1alpha1.ConditionTrue,
				Reason:             "HTTPHeadRequestFailed",
				Message:            fmt.Sprintf("Failed to stat source URL: %v", err),
				LastTransitionTime: metav1.Now(),
			})
		}
		_, err = r.client.TaskV1alpha1().Blobs().Update(ctx, blobCopy, metav1.UpdateOptions{})
		return err
	}

	blobCopy := blob.DeepCopy()
	blobCopy.Spec.Total = fi.Size
	blobCopy.Spec.HandlerName = ""

	if !fi.Range {
		blobCopy.Spec.ChunkSize = 0
	}

	_, err = r.client.TaskV1alpha1().Blobs().Update(ctx, blobCopy, metav1.UpdateOptions{})
	return err
}

func httpStat(url string, client *http.Client, headers map[string]string) (*httpFileInfo, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", versions.DefaultUserAgent())
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, request: %+v, response: %+v, body: %s", resp.StatusCode, req, resp, string(body))
	}

	contentLength := resp.Header.Get("Content-Length")
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Content-Length: %v", err)
	}

	lastModified, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		lastModified = time.Time{} // Use zero time if parsing fails
	}

	return &httpFileInfo{
		Size:    size,
		ModTime: lastModified,
		Range:   resp.Header.Get("Accept-Ranges") == "bytes",
	}, nil
}

type httpFileInfo struct {
	Size    int64
	ModTime time.Time
	Range   bool
}
