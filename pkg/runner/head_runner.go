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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	"github.com/OpenCIDN/cidn/pkg/versions"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	workqueue    workqueue.TypedDelayingInterface[string]
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
		workqueue:    workqueue.NewTypedDelayingQueue[string](),
	}
	r.blobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			blob := obj.(*v1alpha1.Blob)
			key := blob.Name
			r.workqueue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			blob := newObj.(*v1alpha1.Blob)
			key := blob.Name
			r.workqueue.Add(key)
		},
	})

	return r
}

func (r *HeadRunner) Release(ctx context.Context) error {
	blobs, err := r.blobInformer.Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	var wg sync.WaitGroup

	for _, blob := range blobs {
		if blob.Spec.HandlerName != r.handlerName {
			continue
		}

		if blob.Status.Phase != v1alpha1.BlobPhasePending && blob.Status.Phase != v1alpha1.BlobPhaseRunning {
			continue
		}

		wg.Add(1)
		go func(b *v1alpha1.Blob) {
			defer wg.Done()

			blobCopy := b.DeepCopy()
			blobCopy.Spec.HandlerName = ""
			blobCopy.Status.Phase = v1alpha1.BlobPhasePending
			blobCopy.Status.Conditions = nil
			_, err := r.client.TaskV1alpha1().Blobs().Update(ctx, blobCopy, metav1.UpdateOptions{})
			if err != nil {
				if apierrors.IsConflict(err) {
					latest, getErr := r.client.TaskV1alpha1().Blobs().Get(ctx, blobCopy.Name, metav1.GetOptions{})
					if getErr != nil {
						klog.Errorf("failed to get latest blob %s: %v", blobCopy.Name, getErr)
						return
					}
					latest.Spec.HandlerName = ""
					latest.Status.Phase = v1alpha1.BlobPhasePending
					latest.Status.Conditions = nil
					_, err = r.client.TaskV1alpha1().Blobs().Update(ctx, latest, metav1.UpdateOptions{})
					if err != nil {
						klog.Errorf("failed to update blob %s: %v", latest.Name, err)
						return
					}
				}
				klog.Errorf("failed to release blob %s: %v", b.Name, err)
			}
		}(blob)
	}

	wg.Wait()

	return nil
}

func (r *HeadRunner) Shutdown(ctx context.Context) error {
	return r.Release(ctx)
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
	if err != nil {
		r.workqueue.AddAfter(key, 10*time.Second)

		klog.Errorf("Error processing blob %q: %v", key, err)
	}

	return true
}

func (r *HeadRunner) processBlob(ctx context.Context, key string) error {
	blob, err := r.blobInformer.Lister().Get(key)
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
		blobCopy.Status.Phase = v1alpha1.BlobPhaseRunning
		_, err = r.client.TaskV1alpha1().Blobs().Update(ctx, blobCopy, metav1.UpdateOptions{})
		return err
	}

	if blob.Spec.HandlerName != r.handlerName {
		return nil
	}

	fi, retryable, err := httpStat(blob.Spec.Source, r.httpClient, nil)
	if err != nil {
		blobCopy := blob.DeepCopy()
		blobCopy.Status.Phase = v1alpha1.BlobPhaseFailed
		blobCopy.Status.Conditions = v1alpha1.AppendConditions(blobCopy.Status.Conditions,
			v1alpha1.Condition{
				Type:               "HTTPHead",
				Status:             v1alpha1.ConditionTrue,
				Reason:             "HTTPHeadRequestFailed",
				Message:            fmt.Sprintf("Failed to head source URL: %v", err),
				LastTransitionTime: metav1.Now(),
			},
		)

		if retryable {
			blobCopy.Status.Conditions = v1alpha1.AppendConditions(blobCopy.Status.Conditions,
				v1alpha1.Condition{
					Type:               v1alpha1.ConditionTypeRetryable,
					Status:             v1alpha1.ConditionTrue,
					Reason:             "",
					Message:            fmt.Sprintf("Retryable, Retry count: %d", blobCopy.Status.RetryCount),
					LastTransitionTime: metav1.Now(),
				},
			)
		}
		_, err = r.client.TaskV1alpha1().Blobs().Update(context.Background(), blobCopy, metav1.UpdateOptions{})
		return err
	}

	blobCopy := blob.DeepCopy()
	blobCopy.Spec.Total = fi.Size
	blobCopy.Spec.Etag = fi.Etag
	blobCopy.Spec.HandlerName = ""
	blobCopy.Status.Phase = v1alpha1.BlobPhasePending
	if !fi.Range {
		blobCopy.Spec.ChunkSize = 0
	}

	_, err = r.client.TaskV1alpha1().Blobs().Update(context.Background(), blobCopy, metav1.UpdateOptions{})
	return err
}

func httpStat(url string, client *http.Client, headers map[string]string) (*httpFileInfo, bool, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		retry, err := utils.IsNetWorkError(err)
		return nil, retry, err
	}

	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", versions.DefaultUserAgent())
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	retry, err := utils.IsHTTPResponseError(resp, err)
	if err != nil {
		return nil, retry, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode >= http.StatusInternalServerError, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	contentLength := resp.Header.Get("Content-Length")
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return nil, false, fmt.Errorf("invalid Content-Length: %v", err)
	}

	lastModified, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		lastModified = time.Time{} // Use zero time if parsing fails
	}

	return &httpFileInfo{
		Size:    size,
		ModTime: lastModified,
		Range:   resp.Header.Get("Accept-Ranges") == "bytes",
		Etag:    resp.Header.Get("Etag"),
	}, true, nil
}

type httpFileInfo struct {
	Size    int64
	ModTime time.Time
	Range   bool
	Etag    string
}
