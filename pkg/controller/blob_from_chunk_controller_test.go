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
	"net/http"
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
		wantTotal         int64
		wantAcceptRanges  bool
	}{
		{name: "content-length missing", headers: map[string]string{"accept-ranges": "bytes"}, wantTotal: -1},
		{name: "content-length 0", headers: map[string]string{"content-length": "0", "accept-ranges": "bytes"}, wantTotal: -1},
		{name: "accept-ranges missing", headers: map[string]string{"content-length": "1024"}, wantTotal: 1024},
		{name: "accept-ranges bytes", headers: map[string]string{"content-length": "1024", "accept-ranges": "bytes"}, wantTotal: 1024, wantAcceptRanges: true},
		{name: "ForceAcceptRanges", headers: map[string]string{"content-length": "1024"}, forceAcceptRanges: true, wantTotal: 1024, wantAcceptRanges: true},
		{name: "ForceAcceptRanges with content-length missing", headers: map[string]string{}, forceAcceptRanges: true, wantTotal: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.ForceAcceptRanges = tt.forceAcceptRanges
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
