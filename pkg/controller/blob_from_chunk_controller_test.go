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
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/wzshiming/sss"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
			if multipartExists(t, client, blob.Name) {
				t.Error("multipart kept after the commit")
			}
		})
	}
}
