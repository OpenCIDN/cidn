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
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/wzshiming/sss"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

const testHandlerName = "controller-test"

func newTestInformers(t *testing.T, objects ...runtime.Object) (*fake.Clientset, externalversions.SharedInformerFactory) {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	factory := externalversions.NewSharedInformerFactory(client, 0)
	informers := factory.Task().V1alpha1()
	for _, obj := range objects {
		var indexer cache.Indexer
		switch obj.(type) {
		case *v1alpha1.Blob:
			indexer = informers.Blobs().Informer().GetIndexer()
		case *v1alpha1.Chunk:
			indexer = informers.Chunks().Informer().GetIndexer()
		case *v1alpha1.Multipart:
			indexer = informers.Multiparts().Informer().GetIndexer()
		}
		if err := indexer.Add(obj); err != nil {
			t.Fatal(err)
		}
	}
	return client, factory
}

func newTestFromChunkController(t *testing.T, objects ...runtime.Object) (*BlobFromChunkController, *fake.Clientset) {
	t.Helper()
	client, factory := newTestInformers(t, objects...)
	c := NewBlobFromChunkController(testHandlerName, map[string]*sss.SSS{}, client, factory)
	t.Cleanup(c.workqueue.ShutDown)
	return c, client
}

func newTestBlob() *v1alpha1.Blob {
	return &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{Name: "blob"},
		Spec: v1alpha1.BlobSpec{
			Source:         []v1alpha1.BlobSource{{URL: "http://source.example/blob"}},
			Destination:    []v1alpha1.BlobDestination{{Name: "storage", Path: "/blob"}},
			MaximumRunning: 2,
			MaximumPending: 1,
			MaximumRetry:   3,
		},
		Status: v1alpha1.BlobStatus{HandlerName: testHandlerName, Phase: v1alpha1.BlobPhaseRunning},
	}
}

func newTestChunk(name string, blob *v1alpha1.Blob, phase v1alpha1.ChunkPhase) *v1alpha1.Chunk {
	return &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{BlobNameAnnotationKey: blob.Name}},
		Status:     v1alpha1.ChunkStatus{Phase: phase},
	}
}

func TestFromHeadChunkReadsTheHeadResponse(t *testing.T) {
	tests := []struct {
		name              string
		headers           map[string]string
		forceAcceptRanges bool
		acceptRanges      bool
		wantTotal         int64
		wantAcceptRanges  bool
	}{
		{name: "content-length missing", headers: map[string]string{"accept-ranges": "bytes"}, wantTotal: -1},
		{name: "content-length 0", headers: map[string]string{"content-length": "0", "accept-ranges": "bytes"}, wantTotal: -1},
		{name: "accept-ranges missing", headers: map[string]string{"content-length": "1024"}, wantTotal: 1024},
		{name: "accept-ranges bytes", headers: map[string]string{"content-length": "1024", "accept-ranges": "bytes"}, wantTotal: 1024, wantAcceptRanges: true},
		{name: "ForceAcceptRanges", headers: map[string]string{"content-length": "1024"}, forceAcceptRanges: true, wantTotal: 1024, wantAcceptRanges: true},
		{name: "ForceAcceptRanges with content-length missing", headers: map[string]string{}, forceAcceptRanges: true, wantTotal: -1},
		{name: "content-length missing keeps earlier range support", headers: map[string]string{}, acceptRanges: true, wantTotal: -1, wantAcceptRanges: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.ForceAcceptRanges = tt.forceAcceptRanges
			blob.Status.AcceptRanges = tt.acceptRanges
			chunk := newTestChunk(buildHeadChunkName(blob.Name, 0), blob, v1alpha1.ChunkPhaseSucceeded)
			chunk.Status.SourceResponse = &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: tt.headers}
			c, _ := newTestFromChunkController(t, blob, chunk)

			if err := c.fromHeadChunk(context.Background(), blob); err != nil {
				t.Fatal(err)
			}
			if blob.Status.Total != tt.wantTotal || blob.Status.AcceptRanges != tt.wantAcceptRanges {
				t.Errorf("total %d, accept ranges %t; want %d, %t", blob.Status.Total, blob.Status.AcceptRanges, tt.wantTotal, tt.wantAcceptRanges)
			}
		})
	}
}

func TestFromOneChunkTakesAMissingTotalFromAnOKResponse(t *testing.T) {
	tests := []struct {
		name      string
		total     int64
		response  *v1alpha1.ChunkHTTPResponse
		wantTotal int64
	}{
		{name: "total not read yet", response: &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: map[string]string{"content-length": "2048"}}, wantTotal: 2048},
		{name: "unknown total", total: -1, response: &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: map[string]string{"content-length": "2048"}}, wantTotal: 2048},
		{name: "content-length missing", total: -1, response: &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, wantTotal: -1},
		{name: "content-length 0", total: -1, response: &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: map[string]string{"content-length": "0"}}, wantTotal: -1},
		{name: "headers missing", total: -1, response: &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK}, wantTotal: -1},
		{name: "not an OK response", total: -1, response: &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusNotFound, Headers: map[string]string{"content-length": "2048"}}, wantTotal: -1},
		{name: "no response", total: -1, wantTotal: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Status.Total = tt.total
			chunk := newTestChunk(buildFullChunkName(blob.Name), blob, v1alpha1.ChunkPhaseRunning)
			chunk.Status.SourceResponse = tt.response
			c, _ := newTestFromChunkController(t, blob, chunk)

			if err := c.fromOneChunk(context.Background(), blob); err != nil {
				t.Fatal(err)
			}
			if blob.Status.Total != tt.wantTotal {
				t.Errorf("total %d, want %d", blob.Status.Total, tt.wantTotal)
			}
		})
	}
}

func newTestSkipHeadBlob() *v1alpha1.Blob {
	blob := newTestBlob()
	blob.Spec.SkipHead = true
	return blob
}

func newTestFirstChunk(blob *v1alpha1.Blob, phase v1alpha1.ChunkPhase) *v1alpha1.Chunk {
	chunk := newTestChunk("blob:first:"+blob.Name, blob, phase)
	chunk.Spec.ChunkIndex = 1
	chunk.Spec.FirstPart = &v1alpha1.ChunkFirstPart{}
	return chunk
}

func getBlob(t *testing.T, client *fake.Clientset, name string) *v1alpha1.Blob {
	t.Helper()
	blob, err := client.TaskV1alpha1().Blobs().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

func getMultipart(t *testing.T, client *fake.Clientset, name string) *v1alpha1.Multipart {
	t.Helper()
	mp, err := client.TaskV1alpha1().Multiparts().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return mp
}

func TestSkipHeadBlobTakesItsTotalFromTheRunningFirstChunk(t *testing.T) {
	tests := []struct {
		name              string
		chunksNumber      int64
		headers           map[string]string
		forceAcceptRanges bool
		wantAcceptRanges  bool
	}{
		{name: "accept-ranges bytes", headers: map[string]string{"content-length": "1048576", "accept-ranges": "bytes"}, wantAcceptRanges: true},
		{name: "accept-ranges missing", headers: map[string]string{"content-length": "1048576"}},
		{name: "ForceAcceptRanges of the first part", headers: map[string]string{"content-length": "1048576"}, forceAcceptRanges: true, wantAcceptRanges: true},
		{name: "one chunk preset", chunksNumber: 1, headers: map[string]string{"content-length": "1048576", "accept-ranges": "bytes"}, wantAcceptRanges: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			blob.Spec.ChunksNumber = tt.chunksNumber
			blob.Status.Phase = v1alpha1.BlobPhaseUnknown
			chunk := newTestFirstChunk(blob, v1alpha1.ChunkPhaseRunning)
			chunk.Spec.FirstPart.ForceAcceptRanges = tt.forceAcceptRanges
			chunk.Status.SourceResponse = &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: tt.headers}
			c, client := newTestFromChunkController(t, blob, chunk)

			c.handler(context.Background(), blob.Name)

			got := getBlob(t, client, blob.Name)
			if got.Status.Total != 1048576 || got.Status.AcceptRanges != tt.wantAcceptRanges {
				t.Errorf("total %d, accept ranges %t; want 1048576, %t", got.Status.Total, got.Status.AcceptRanges, tt.wantAcceptRanges)
			}
			if got.Status.Phase != v1alpha1.BlobPhaseRunning {
				t.Errorf("phase %s, want %s", got.Status.Phase, v1alpha1.BlobPhaseRunning)
			}
		})
	}
}

func TestSkipHeadBlobTakesNoTotalFromAFailedAttemptsResponse(t *testing.T) {
	tests := []struct {
		statusCode int
		retryable  bool
	}{
		{statusCode: http.StatusUnauthorized, retryable: true},
		{statusCode: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.statusCode), func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			chunk := newTestFirstChunk(blob, v1alpha1.ChunkPhaseFailed)
			chunk.Status.Retryable = tt.retryable
			chunk.Status.SourceResponse = &v1alpha1.ChunkHTTPResponse{StatusCode: tt.statusCode, Headers: map[string]string{"content-length": "153", "accept-ranges": "bytes"}}
			c, client := newTestFromChunkController(t, blob, chunk)

			c.handler(context.Background(), blob.Name)

			if got := getBlob(t, client, blob.Name); got.Status.Total != 0 || got.Status.AcceptRanges {
				t.Errorf("total %d, accept ranges %t; want 0, false", got.Status.Total, got.Status.AcceptRanges)
			}
		})
	}
}

func TestSkipHeadBlobIsQueuedAtOnceWhenItsTotalIsWritten(t *testing.T) {
	withoutSkipHead := newTestBlob()
	withoutSkipHead.Name = "without-skiphead"
	waiting := newTestSkipHeadBlob()
	waiting.Name = "waiting"
	laidOut := newTestLaidOutSkipHeadBlob()
	laidOut.Name = "laid-out"
	started := newTestSkipHeadBlob()
	started.Name = "started"
	client := fake.NewSimpleClientset(withoutSkipHead, waiting, laidOut, started)
	var once sync.Once
	watching := make(chan struct{})
	client.PrependWatchReactor("blobs", func(action clienttesting.Action) (bool, watch.Interface, error) {
		w, err := client.Tracker().Watch(action.GetResource(), action.GetNamespace())
		once.Do(func() { close(watching) })
		return true, w, err
	})
	factory := externalversions.NewSharedInformerFactory(client, 0)
	c := NewBlobFromChunkController(testHandlerName, map[string]*sss.SSS{}, client, factory)
	t.Cleanup(c.workqueue.ShutDown)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	select {
	case <-watching:
	case <-time.After(5 * time.Second):
		t.Fatal("blobs never watched")
	}

	withoutSkipHead.Status.Total = 1024
	waiting.Status.Retry = 1
	laidOut.Status.Progress = 10
	started.Status.Total = 1024
	// Events arrive in order, so the other updates were handled once the last one is queued.
	for _, blob := range []*v1alpha1.Blob{withoutSkipHead, waiting, laidOut, started} {
		if _, err := client.TaskV1alpha1().Blobs().UpdateStatus(ctx, blob, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	if err := wait.PollUntilContextTimeout(ctx, 5*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		return c.workqueue.Len() != 0, nil
	}); err != nil {
		t.Fatal("no blob queued before the 10 s requeue")
	}
	if key, _ := c.workqueue.Get(); key != started.Name || c.workqueue.Len() != 0 {
		t.Errorf("queued %q and %d more, want only %q", key, c.workqueue.Len(), started.Name)
	}
}

func TestSkipHeadBlobFailsWithTheConditionsOfItsFailedFirstChunk(t *testing.T) {
	for _, total := range []int64{0, 1024} {
		t.Run(fmt.Sprintf("total %d", total), func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			blob.Spec.ChunksNumber, blob.Status.Total = 1, total
			chunk := newTestFirstChunk(blob, v1alpha1.ChunkPhaseFailed)
			condition := v1alpha1.Condition{Type: "UnexpectedStatusCode", Message: "unexpected status code: got 404, want 200"}
			chunk.Status.Conditions = []v1alpha1.Condition{condition}
			c, client := newTestFromChunkController(t, blob, chunk)

			c.handler(context.Background(), blob.Name)

			got := getBlob(t, client, blob.Name)
			if got.Status.Phase != v1alpha1.BlobPhaseFailed {
				t.Errorf("phase %s, want %s", got.Status.Phase, v1alpha1.BlobPhaseFailed)
			}
			if !reflect.DeepEqual(got.Status.Conditions, []v1alpha1.Condition{condition}) {
				t.Errorf("conditions %v, want %v", got.Status.Conditions, condition)
			}
		})
	}
}

func TestSkipHeadBlobWaitsForTheRetryOfItsFirstChunk(t *testing.T) {
	blob := newTestSkipHeadBlob()
	chunk := newTestFirstChunk(blob, v1alpha1.ChunkPhaseFailed)
	chunk.Status.Retryable, chunk.Status.Retry = true, 2
	c, client := newTestFromChunkController(t, blob, chunk)

	c.handler(context.Background(), blob.Name)

	if got := getBlob(t, client, blob.Name); got.Status.Phase != v1alpha1.BlobPhaseRunning || got.Status.Retry != 2 {
		t.Errorf("phase %s, retry %d; want %s, 2", got.Status.Phase, got.Status.Retry, v1alpha1.BlobPhaseRunning)
	}
}

func TestFailedFirstChunkRemovesTheUploadOnlyWhileNoPartIsRecorded(t *testing.T) {
	tests := []struct {
		name          string
		recorded      []v1alpha1.UploadEtags
		wantMultipart bool
	}{
		{name: "no part recorded", recorded: make([]v1alpha1.UploadEtags, 1)},
		{name: "part 2 recorded", recorded: []v1alpha1.UploadEtags{{}, {Size: 10, Etags: []string{"etag-2"}}, {}}, wantMultipart: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			blob.Spec.ChunksNumber = int64(len(tt.recorded))
			blob.Status.Total = 30
			mp := newTestMultipart(blob, 0, "upload-1")
			mp.UploadEtags = tt.recorded
			c, client := newTestFromChunkController(t, blob, newTestFirstChunk(blob, v1alpha1.ChunkPhaseFailed), mp)

			c.handler(context.Background(), blob.Name)

			if got := multipartExists(t, client, blob.Name); got != tt.wantMultipart {
				t.Errorf("multipart kept %t, want %t", got, tt.wantMultipart)
			}
		})
	}
}

func newTestSucceededFirstChunk(blob *v1alpha1.Blob, took int64, etags ...string) *v1alpha1.Chunk {
	chunk := newTestFirstChunk(blob, v1alpha1.ChunkPhaseSucceeded)
	chunk.Status.SourceProgress, chunk.Status.Etags = took, etags
	return chunk
}

func TestSkipHeadBlobSucceedsWhenItsFirstChunkSkipped(t *testing.T) {
	blob := newTestSkipHeadBlob()
	blob.Spec.ChunksNumber, blob.Status.Total = 1, 1024
	blob.Status.RunningChunks = 1
	c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, 0), newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	got := getBlob(t, client, blob.Name)
	if got.Status.Phase != v1alpha1.BlobPhaseSucceeded {
		t.Errorf("phase %s, want %s", got.Status.Phase, v1alpha1.BlobPhaseSucceeded)
	}
	if got.Status.Progress != 1024 || got.Status.SucceededChunks != 1 || got.Status.RunningChunks != 0 {
		t.Errorf("progress %d, succeeded chunks %d, running chunks %d; want 1024, 1, 0", got.Status.Progress, got.Status.SucceededChunks, got.Status.RunningChunks)
	}
}

func TestSkipHeadBlobFailsWhenItsFirstChunkTookAnotherSize(t *testing.T) {
	blob := newTestSkipHeadBlob()
	blob.Spec.ChunksNumber, blob.Status.Total = 1, 1024
	c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, 1000, "etag-1"), newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	got := getBlob(t, client, blob.Name)
	if got.Status.Phase != v1alpha1.BlobPhaseFailed {
		t.Errorf("phase %s, want %s", got.Status.Phase, v1alpha1.BlobPhaseFailed)
	}
	if len(got.Status.Conditions) != 1 || !strings.Contains(got.Status.Conditions[0].Message, "1000") || !strings.Contains(got.Status.Conditions[0].Message, "1024") {
		t.Errorf("conditions %v, want one naming 1000 and 1024", got.Status.Conditions)
	}
}

func TestSkipHeadBlobCommitsItsFirstChunkAsItsOnlyPart(t *testing.T) {
	tests := []struct {
		name      string
		total     int64
		took      int64
		wantTotal int64
	}{
		{name: "known total", total: 1024, took: 1024, wantTotal: 1024},
		{name: "unknown total", total: -1, took: 1500, wantTotal: 1500},
		{name: "empty content", total: -1, took: 0, wantTotal: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			blob.Spec.ChunksNumber, blob.Status.Total = 1, tt.total
			// An empty upload ID makes the commit skip the store.
			c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, tt.took, "etag-1"), newTestMultipart(blob, 1, ""))

			c.handler(context.Background(), blob.Name)

			got := getBlob(t, client, blob.Name)
			if got.Status.Phase != v1alpha1.BlobPhaseSucceeded {
				t.Errorf("phase %s, conditions %v; want %s", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseSucceeded)
			}
			if got.Status.Total != tt.wantTotal || got.Status.Progress != tt.took {
				t.Errorf("total %d, progress %d; want %d, %d", got.Status.Total, got.Status.Progress, tt.wantTotal, tt.took)
			}
			want := []v1alpha1.UploadEtags{{Size: tt.took, Etags: []string{"etag-1"}}}
			if mp := getMultipart(t, client, blob.Name); !reflect.DeepEqual(mp.UploadEtags, want) || !reflect.DeepEqual(mp.UploadIDs, []string{""}) {
				t.Errorf("multipart parts %+v, upload IDs %q; want %+v and one empty upload ID", mp.UploadEtags, mp.UploadIDs, want)
			}
		})
	}
}

func TestUnknownTotalIsCheckedAgainstTheFirstChunksOwnResponse(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantCommit bool
	}{
		{name: "response with a larger total", headers: map[string]string{"content-length": "16777216", "accept-ranges": "bytes"}},
		{name: "response with the bytes taken as total", headers: map[string]string{"content-length": "5592406"}, wantCommit: true},
		{name: "response without a total", headers: map[string]string{}, wantCommit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			blob.Spec.ChunksNumber, blob.Status.Total = 1, -1
			first := newTestSucceededFirstChunk(blob, 5592406, "etag-1")
			first.Status.SourceResponse = &v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK, Headers: tt.headers}
			// An empty upload ID makes the commit skip the store.
			c, client := newTestFromChunkController(t, blob, first, newTestMultipart(blob, 1, ""))

			c.handler(context.Background(), blob.Name)

			got := getBlob(t, client, blob.Name)
			// The commit leaves every part recorded in the multipart.
			if committed := len(getMultipart(t, client, blob.Name).UploadEtags[0].Etags) != 0; committed != tt.wantCommit {
				t.Fatalf("committed %t, phase %s, conditions %v; want committed %t", committed, got.Status.Phase, got.Status.Conditions, tt.wantCommit)
			}
			if tt.wantCommit {
				if got.Status.Phase != v1alpha1.BlobPhaseSucceeded || got.Status.Total != 5592406 {
					t.Errorf("phase %s, total %d; want %s, 5592406", got.Status.Phase, got.Status.Total, v1alpha1.BlobPhaseSucceeded)
				}
				return
			}
			if got.Status.Phase != v1alpha1.BlobPhaseFailed || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != "FirstPartSizeMismatch" ||
				!strings.Contains(got.Status.Conditions[0].Message, "5592406") || !strings.Contains(got.Status.Conditions[0].Message, "16777216") {
				t.Errorf("phase %s, conditions %v; want %s with a FirstPartSizeMismatch naming 5592406 and 16777216", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseFailed)
			}
		})
	}
}

func newTestRecordedPart(blob *v1alpha1.Blob, name string, num int64) *v1alpha1.Chunk {
	chunk := newTestChunk(name, blob, v1alpha1.ChunkPhaseSucceeded)
	chunk.Spec.ChunkIndex, chunk.Spec.Total = num, blob.Spec.ChunkSize
	chunk.Status.Etags = []string{fmt.Sprintf("etag-%d", num)}
	return chunk
}

func TestLaidOutSkipHeadBlobCommitsItsPartsOnceItsFirstChunkIsGone(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	// An empty upload ID makes the commit skip the store.
	mp := newTestMultipart(blob, 0, "")
	mp.UploadEtags = []v1alpha1.UploadEtags{{Size: 10, Etags: []string{"etag-1"}}, {Size: 10, Etags: []string{"etag-2"}}, {Size: 10, Etags: []string{"etag-3"}}}
	c, client := newTestFromChunkController(t, blob, newTestRecordedPart(blob, "blob:part:blob:3:14-1e", 3), mp)

	c.handler(context.Background(), blob.Name)

	got := getBlob(t, client, blob.Name)
	if got.Status.Phase != v1alpha1.BlobPhaseSucceeded || got.Status.Progress != 30 {
		t.Errorf("phase %s, progress %d, conditions %v; want %s, 30", got.Status.Phase, got.Status.Progress, got.Status.Conditions, v1alpha1.BlobPhaseSucceeded)
	}
}

func TestSkipHeadBlobRecordsAFirstChunkOfOneLaidOutChunkAsPartOne(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, 10, "etag-1"), newTestMultipart(blob, 3, "upload-1"))

	c.handler(context.Background(), blob.Name)

	got := getBlob(t, client, blob.Name)
	if got.Status.Phase != v1alpha1.BlobPhaseRunning || got.Status.SucceededChunks != 1 || got.Status.Progress != 10 {
		t.Errorf("phase %s, succeeded chunks %d, progress %d, conditions %v; want %s, 1, 10", got.Status.Phase, got.Status.SucceededChunks, got.Status.Progress, got.Status.Conditions, v1alpha1.BlobPhaseRunning)
	}
}

func TestSkipHeadBlobFailsWhenItsFirstChunkTookMoreThanALaidOutChunk(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, 30, "etag-1"), newTestMultipart(blob, 3, "upload-1"))

	c.handler(context.Background(), blob.Name)

	got := getBlob(t, client, blob.Name)
	if got.Status.Phase != v1alpha1.BlobPhaseFailed {
		t.Errorf("phase %s, want %s", got.Status.Phase, v1alpha1.BlobPhaseFailed)
	}
	if len(got.Status.Conditions) != 1 || !strings.Contains(got.Status.Conditions[0].Message, "30") || !strings.Contains(got.Status.Conditions[0].Message, "10") {
		t.Errorf("conditions %v, want one naming 30 and 10", got.Status.Conditions)
	}
}

// waitForUploadEtags waits for the multipart fromChunks updates in the background.
func waitForUploadEtags(t *testing.T, client *fake.Clientset, name string, parts int) *v1alpha1.Multipart {
	t.Helper()
	var mp *v1alpha1.Multipart
	if err := wait.PollUntilContextTimeout(context.Background(), 5*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		var err error
		mp, err = client.TaskV1alpha1().Multiparts().Get(ctx, name, metav1.GetOptions{})
		return err == nil && len(mp.UploadEtags) == parts, err
	}); err != nil {
		t.Fatalf("multipart %+v, want %d parts: %v", mp, parts, err)
	}
	return mp
}

func TestLaidOutSkipHeadBlobGrowsItsMultipartToItsNumberOfParts(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	partOne := v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	mp := newTestMultipart(blob, 1, "upload-1")
	mp.UploadEtags[0] = partOne
	partTwo := newTestChunk("blob:part:blob:2:0a-14", blob, v1alpha1.ChunkPhaseRunning)
	partTwo.Spec.ChunkIndex = 2
	c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, 10, "etag-1"), partTwo, mp)

	c.handler(context.Background(), blob.Name)

	got := waitForUploadEtags(t, client, blob.Name, 3)
	if !reflect.DeepEqual(got.UploadIDs, []string{"upload-1"}) || !reflect.DeepEqual(got.UploadEtags[0], partOne) {
		t.Errorf("upload IDs %v, part 1 %+v; want [upload-1], %+v", got.UploadIDs, got.UploadEtags[0], partOne)
	}
}

func TestSkipHeadBlobRecordsAPartItsMultipartDidNotHoldYet(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	mp := newTestMultipart(blob, 1, "upload-1")
	mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	c, client := newTestFromChunkController(t, blob, newTestRecordedPart(blob, "blob:part:blob:2:0a-14", 2), mp)

	c.handler(context.Background(), blob.Name)

	if got := getBlob(t, client, blob.Name); got.Status.SucceededChunks != 2 || got.Status.Progress != 20 {
		t.Errorf("succeeded chunks %d, progress %d; want 2, 20", got.Status.SucceededChunks, got.Status.Progress)
	}
}

func TestCommitFailsABlobWhoseMultipartLacksAnEtagOrUploadID(t *testing.T) {
	two := []v1alpha1.BlobDestination{{Name: "storage", Path: "/blob"}, {Name: "second", Path: "/blob"}}
	tests := []struct {
		name        string
		destination []v1alpha1.BlobDestination
		uploadIDs   []string
		slots       int64
		etags       []string
	}{
		{name: "more slots than parts", uploadIDs: []string{"upload-1"}, slots: 3, etags: []string{"etag-1"}},
		{name: "fewer etags than destinations", destination: two, uploadIDs: []string{"upload-1", "upload-2"}, slots: 1, etags: []string{"etag-1"}},
		{name: "fewer upload IDs than destinations", destination: two, uploadIDs: []string{"upload-1"}, slots: 1, etags: []string{"etag-1", "etag-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			if tt.destination != nil {
				blob.Spec.Destination = tt.destination
			}
			blob.Spec.ChunksNumber, blob.Status.Total = 1, 1024
			stores := newTestStorage(t)
			stores["second"] = stores["storage"]
			client, factory := newTestInformers(t, blob, newTestSucceededFirstChunk(blob, 1024, tt.etags...), newTestMultipart(blob, tt.slots, tt.uploadIDs...))
			c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
			t.Cleanup(c.workqueue.ShutDown)

			c.handler(context.Background(), blob.Name)

			got := getBlob(t, client, blob.Name)
			if got.Status.Phase != v1alpha1.BlobPhaseFailed || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != "MultipartIncomplete" {
				t.Errorf("phase %s, conditions %v; want %s with MultipartIncomplete", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseFailed)
			}
			if !multipartExists(t, client, blob.Name) {
				t.Error("multipart committed")
			}
		})
	}
}

func TestFailedFirstChunkRemovesTheUnfinishedParts(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	blob.Status.RunningChunks, blob.Status.PendingChunks = 2, 1
	first := newTestFirstChunk(blob, v1alpha1.ChunkPhaseFailed)
	first.Status.Conditions = []v1alpha1.Condition{{Type: "UnexpectedStatusCode", Message: "unexpected status code: got 404, want 200"}}
	partTwo := newTestChunk("blob:part:blob:2:0a-14", blob, v1alpha1.ChunkPhaseRunning)
	partThree := newTestChunk("blob:part:blob:3:14-1e", blob, v1alpha1.ChunkPhasePending)
	c, client := newTestFromChunkController(t, blob, first, partTwo, partThree, newTestMultipart(blob, 3, "upload-1"))

	c.handler(context.Background(), blob.Name)

	if got, want := chunkNames(t, client), []string{"blob:first:blob"}; !reflect.DeepEqual(got, want) {
		t.Errorf("chunks %v, want %v", got, want)
	}
	if got := getBlob(t, client, blob.Name); got.Status.Phase != v1alpha1.BlobPhaseFailed || got.Status.RunningChunks != 0 || got.Status.PendingChunks != 0 {
		t.Errorf("phase %s, running chunks %d, pending chunks %d; want %s, 0, 0", got.Status.Phase, got.Status.RunningChunks, got.Status.PendingChunks, v1alpha1.BlobPhaseFailed)
	}
}

func TestFirstChunkOfAnotherSizeRemovesTheUnfinishedParts(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	blob.Status.RunningChunks, blob.Status.PendingChunks = 2, 1
	partTwo := newTestChunk("blob:part:blob:2:0a-14", blob, v1alpha1.ChunkPhaseRunning)
	partThree := newTestChunk("blob:part:blob:3:14-1e", blob, v1alpha1.ChunkPhasePending)
	c, client := newTestFromChunkController(t, blob, newTestSucceededFirstChunk(blob, 30, "etag-1"), partTwo, partThree, newTestMultipart(blob, 3, "upload-1"))

	c.handler(context.Background(), blob.Name)

	if got, want := chunkNames(t, client), []string{"blob:first:blob"}; !reflect.DeepEqual(got, want) {
		t.Errorf("chunks %v, want %v", got, want)
	}
	got := getBlob(t, client, blob.Name)
	if got.Status.Phase != v1alpha1.BlobPhaseFailed || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != "FirstPartSizeMismatch" {
		t.Errorf("phase %s, conditions %v; want %s with FirstPartSizeMismatch", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseFailed)
	}
	if got.Status.RunningChunks != 0 || got.Status.PendingChunks != 0 {
		t.Errorf("running chunks %d, pending chunks %d; want 0, 0", got.Status.RunningChunks, got.Status.PendingChunks)
	}
}

func TestCommitLeavesTheMultipartWithEveryPartAndNoUploadID(t *testing.T) {
	withoutSkipHead := newTestBlob()
	withoutSkipHead.Spec.ChunksNumber, withoutSkipHead.Spec.ChunkSize = 3, 10
	withoutSkipHead.Status.Total, withoutSkipHead.Status.AcceptRanges = 30, true
	laidOut := newTestLaidOutSkipHeadBlob()
	onePart := newTestSkipHeadBlob()
	onePart.Spec.ChunksNumber, onePart.Status.Total = 1, 1024
	threeParts := []v1alpha1.UploadEtags{{Size: 10, Etags: []string{"etag-1"}}, {Size: 10, Etags: []string{"etag-2"}}, {Size: 10, Etags: []string{"etag-3"}}}
	tests := []struct {
		name      string
		blob      *v1alpha1.Blob
		lastChunk *v1alpha1.Chunk
		want      []v1alpha1.UploadEtags
	}{
		{name: "three parts without skipHead", blob: withoutSkipHead, lastChunk: newTestRecordedPart(withoutSkipHead, "blob:part:blob:3:14-1e", 3), want: threeParts},
		{name: "three parts of skipHead after its first chunk", blob: laidOut, lastChunk: newTestRecordedPart(laidOut, "blob:part:blob:3:14-1e", 3), want: threeParts},
		{name: "one part of skipHead", blob: onePart, lastChunk: newTestSucceededFirstChunk(onePart, 1024, "etag-1"), want: []v1alpha1.UploadEtags{{Size: 1024, Etags: []string{"etag-1"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := newTestMultipart(tt.blob, int64(len(tt.want)), "upload-1")
			// The multipart holds every part but the last, whose chunk succeeded.
			copy(mp.UploadEtags, tt.want[:len(tt.want)-1])
			stores, requests := newTestUploadStorage(t, "upload-1")
			client, factory := newTestInformers(t, tt.blob, tt.lastChunk, mp)
			c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
			t.Cleanup(c.workqueue.ShutDown)

			c.handler(context.Background(), tt.blob.Name)

			if got := getBlob(t, client, tt.blob.Name); got.Status.Phase != v1alpha1.BlobPhaseSucceeded {
				t.Errorf("phase %s, conditions %v; want %s", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseSucceeded)
			}
			if got, want := requests(), []string{"POST /bucket/blob?uploadId=upload-1"}; !reflect.DeepEqual(got, want) {
				t.Errorf("store requests %q, want %q", got, want)
			}
			for _, action := range client.Actions() {
				if action.Matches("delete", "multiparts") {
					t.Error("multipart deleted")
				}
			}
			got := getMultipart(t, client, tt.blob.Name)
			if !reflect.DeepEqual(got.UploadEtags, tt.want) || !reflect.DeepEqual(got.UploadIDs, []string{""}) {
				t.Errorf("multipart parts %+v, upload IDs %q; want %+v and one empty upload ID", got.UploadEtags, got.UploadIDs, tt.want)
			}
		})
	}
}

func TestPassAfterTheCommitSucceedsWithoutCommittingAgain(t *testing.T) {
	blob := newTestBlob()
	blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
	blob.Status.Total, blob.Status.AcceptRanges = 30, true
	mp := newTestMultipart(blob, 3, "upload-1")
	mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	mp.UploadEtags[1] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-2"}}
	stores, requests := newTestUploadStorage(t, "upload-1")
	client, factory := newTestInformers(t, blob, newTestRecordedPart(blob, "blob:part:blob:3:14-1e", 3), mp)
	c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
	t.Cleanup(c.workqueue.ShutDown)
	// A pass that commits, but whose Succeeded never gets written.
	if err := c.fromChunks(context.Background(), blob.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	committed := len(requests())
	if err := factory.Task().V1alpha1().Multiparts().Informer().GetIndexer().Update(getMultipart(t, client, blob.Name)); err != nil {
		t.Fatal(err)
	}

	c.handler(context.Background(), blob.Name)

	if got := requests()[committed:]; len(got) != 0 {
		t.Errorf("store requests %q after the commit, want none", got)
	}
	if got := getBlob(t, client, blob.Name); got.Status.Phase != v1alpha1.BlobPhaseSucceeded {
		t.Errorf("phase %s, conditions %v; want %s", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseSucceeded)
	}
}

func TestCommitIsRecordedInTheMultipartBeforeTheSha256Check(t *testing.T) {
	blob := newTestBlob()
	blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
	blob.Status.Total, blob.Status.AcceptRanges = 30, true
	blob.Spec.ContentSha256, blob.Spec.Destination[0].ReverifySha256 = "sha", true
	mp := newTestMultipart(blob, 3, "upload-1")
	mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	mp.UploadEtags[1] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-2"}}
	stores, requests := newTestUploadStorage(t, "upload-1")
	client, factory := newTestInformers(t, blob, newTestRecordedPart(blob, "blob:part:blob:3:14-1e", 3), mp)
	var beforeRewrite []string
	client.PrependReactor("update", "multiparts", func(clienttesting.Action) (bool, runtime.Object, error) {
		beforeRewrite = requests()
		return false, nil, nil
	})
	c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
	t.Cleanup(c.workqueue.ShutDown)

	c.handler(context.Background(), blob.Name)

	if got := getBlob(t, client, blob.Name); got.Status.Phase != v1alpha1.BlobPhaseFailed || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != "Sha256Verification" {
		t.Errorf("phase %s, conditions %v; want %s with Sha256Verification", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseFailed)
	}
	if want := []string{"POST /bucket/blob?uploadId=upload-1"}; !reflect.DeepEqual(beforeRewrite, want) {
		t.Errorf("store requests %q when the multipart was rewritten, want %q", beforeRewrite, want)
	}
	want := []v1alpha1.UploadEtags{{Size: 10, Etags: []string{"etag-1"}}, {Size: 10, Etags: []string{"etag-2"}}, {Size: 10, Etags: []string{"etag-3"}}}
	if got := getMultipart(t, client, blob.Name); !reflect.DeepEqual(got.UploadEtags, want) || !reflect.DeepEqual(got.UploadIDs, []string{""}) {
		t.Errorf("multipart parts %+v, upload IDs %q; want %+v and one empty upload ID", got.UploadEtags, got.UploadIDs, want)
	}
}

func TestCommitIsRecordedOverAConcurrentWrite(t *testing.T) {
	blob := newTestBlob()
	blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
	blob.Status.Total, blob.Status.AcceptRanges = 30, true
	mp := newTestMultipart(blob, 3, "upload-1")
	mp.ResourceVersion = "1"
	mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	stores, _ := newTestUploadStorage(t, "upload-1")
	client, factory := newTestInformers(t, blob, newTestRecordedPart(blob, "blob:part:blob:2:0a-14", 2), newTestRecordedPart(blob, "blob:part:blob:3:14-1e", 3), mp)
	// An earlier pass's write of part 2 reached the API server but not yet the cache.
	latest := mp.DeepCopy()
	latest.ResourceVersion = "2"
	latest.UploadEtags[1] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-2"}}
	if err := client.Tracker().Update(v1alpha1.SchemeGroupVersion.WithResource("multiparts"), latest, ""); err != nil {
		t.Fatal(err)
	}
	// Like the API server, refuse a write based on an older version.
	client.PrependReactor("update", "multiparts", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.(clienttesting.UpdateAction).GetObject().(*v1alpha1.Multipart).ResourceVersion == latest.ResourceVersion {
			return false, nil, nil
		}
		return true, nil, apierrors.NewConflict(v1alpha1.Resource("multiparts"), mp.Name, errors.New("the object has been modified"))
	})
	c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
	t.Cleanup(c.workqueue.ShutDown)

	c.handler(context.Background(), blob.Name)

	want := []v1alpha1.UploadEtags{{Size: 10, Etags: []string{"etag-1"}}, {Size: 10, Etags: []string{"etag-2"}}, {Size: 10, Etags: []string{"etag-3"}}}
	if got := getMultipart(t, client, blob.Name); !reflect.DeepEqual(got.UploadEtags, want) || !reflect.DeepEqual(got.UploadIDs, []string{""}) {
		t.Errorf("multipart parts %+v, upload IDs %q; want %+v and one empty upload ID", got.UploadEtags, got.UploadIDs, want)
	}
}

func TestBlobSucceedsWhenItsCommitCannotBeRecorded(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "conflict on every write", err: apierrors.NewConflict(v1alpha1.Resource("multiparts"), "blob", errors.New("the object has been modified"))},
		{name: "server error", err: apierrors.NewInternalError(errors.New("etcd unavailable"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
			blob.Status.Total, blob.Status.AcceptRanges = 30, true
			mp := newTestMultipart(blob, 3, "upload-1")
			mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
			mp.UploadEtags[1] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-2"}}
			stores, _ := newTestUploadStorage(t, "upload-1")
			client, factory := newTestInformers(t, blob, newTestRecordedPart(blob, "blob:part:blob:3:14-1e", 3), mp)
			client.PrependReactor("update", "multiparts", func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, tt.err
			})
			c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
			t.Cleanup(c.workqueue.ShutDown)

			c.handler(context.Background(), blob.Name)

			if got := getBlob(t, client, blob.Name); got.Status.Phase != v1alpha1.BlobPhaseSucceeded {
				t.Errorf("phase %s, conditions %v; want %s", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseSucceeded)
			}
			if got := getMultipart(t, client, blob.Name); !reflect.DeepEqual(got.UploadEtags, mp.UploadEtags) || !reflect.DeepEqual(got.UploadIDs, mp.UploadIDs) {
				t.Errorf("multipart parts %+v, upload IDs %q; want %+v and %q as before", got.UploadEtags, got.UploadIDs, mp.UploadEtags, mp.UploadIDs)
			}
		})
	}
}

func TestRejectedCommitFailsTheBlobAndKeepsItsUploadIDs(t *testing.T) {
	two := []v1alpha1.BlobDestination{{Name: "storage", Path: "/blob"}, {Name: "second", Path: "/blob"}}
	tests := []struct {
		name        string
		destination []v1alpha1.BlobDestination
		uploadIDs   []string
		etags       []string
	}{
		{name: "one destination", uploadIDs: []string{"upload-2"}, etags: []string{"etag-1"}},
		{name: "two destinations, one rejected", destination: two, uploadIDs: []string{"upload-1", "upload-2"}, etags: []string{"etag-1", "etag-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			if tt.destination != nil {
				blob.Spec.Destination = tt.destination
			}
			blob.Spec.ChunksNumber, blob.Status.Total = 1, 1024
			// The store opened upload-1 only, so it rejects the commit of upload-2.
			stores, _ := newTestUploadStorage(t, "upload-1")
			stores["second"] = stores["storage"]
			client, factory := newTestInformers(t, blob, newTestSucceededFirstChunk(blob, 1024, tt.etags...), newTestMultipart(blob, 1, tt.uploadIDs...))
			c := NewBlobFromChunkController(testHandlerName, stores, client, factory)
			t.Cleanup(c.workqueue.ShutDown)

			c.handler(context.Background(), blob.Name)

			got := getBlob(t, client, blob.Name)
			if got.Status.Phase != v1alpha1.BlobPhaseFailed || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != "MultipartCommit" {
				t.Errorf("phase %s, conditions %v; want %s with MultipartCommit", got.Status.Phase, got.Status.Conditions, v1alpha1.BlobPhaseFailed)
			}
			if got := getMultipart(t, client, blob.Name).UploadIDs; !reflect.DeepEqual(got, tt.uploadIDs) {
				t.Errorf("upload IDs %q, want %q kept", got, tt.uploadIDs)
			}
		})
	}
}
