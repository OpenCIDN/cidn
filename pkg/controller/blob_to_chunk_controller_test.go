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
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/wzshiming/sss"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func newTestToChunkController(t *testing.T, s3 map[string]*sss.SSS, objects ...runtime.Object) (*BlobToChunkController, *fake.Clientset) {
	t.Helper()
	client, factory := newTestInformers(t, objects...)
	c := NewBlobToChunkController(testHandlerName, s3, client, factory)
	t.Cleanup(c.workqueue.ShutDown)
	return c, client
}

// Presigning needs no store, so this client never connects.
func newTestStorage(t *testing.T) map[string]*sss.SSS {
	t.Helper()
	s, err := sss.NewSSS(sss.WithAccessKey("key"), sss.WithSecretKey("secret"), sss.WithBucket("bucket"), sss.WithRegion("us-east-1"), sss.WithRegionEndpoint("http://storage.invalid"), sss.WithForcePathStyle(true))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*sss.SSS{"storage": s}
}

// checkPartURLs checks that each destination uploads the part and clears the signed URLs.
func checkPartURLs(t *testing.T, chunk *v1alpha1.Chunk, num int64, uploadIDs ...string) {
	t.Helper()
	if len(chunk.Spec.Destination) != len(uploadIDs) {
		t.Fatalf("%d destinations, want %d", len(chunk.Spec.Destination), len(uploadIDs))
	}
	for i := range chunk.Spec.Destination {
		u, err := url.Parse(chunk.Spec.Destination[i].Request.URL)
		if err != nil {
			t.Fatal(err)
		}
		if u.Path != "/bucket/blob" || u.Query().Get("partNumber") != strconv.FormatInt(num, 10) || u.Query().Get("uploadId") != uploadIDs[i] {
			t.Errorf("destination %d URL %s, want part %d of upload %s of /bucket/blob", i, u, num, uploadIDs[i])
		}
		chunk.Spec.Destination[i].Request.URL = ""
	}
}

func TestBuildChunkMakesARangePartOfTheUpload(t *testing.T) {
	blob := newTestBlob()
	blob.Annotations = map[string]string{v1alpha1.WebuiGroupAnnotation: "group", v1alpha1.WebuiDisplayNameAnnotation: "layer"}
	blob.Spec.Source = []v1alpha1.BlobSource{
		{URL: "http://source.example/blob", BearerName: "bearer", Headers: map[string]string{"x-token": "token"}},
		{URL: "http://mirror.example/blob"},
	}
	blob.Spec.Priority = 7
	blob.Spec.ContentSha256 = "sha"
	blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
	blob.Status.Total, blob.Status.Retry = 30, 1
	c, _ := newTestToChunkController(t, newTestStorage(t))

	chunk, err := c.buildChunk(blob, "part-2", 2, 10, 20, "part-1", []string{"upload-1"})
	if err != nil {
		t.Fatal(err)
	}

	checkPartURLs(t, chunk, 2, "upload-1")
	want := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "part-2",
			Labels: map[string]string{},
			Annotations: map[string]string{
				BlobNameAnnotationKey:                   "blob",
				v1alpha1.WebuiGroupAnnotation:           "group",
				v1alpha1.WebuiGroupIgnoreSizeAnnotation: "true",
				v1alpha1.WebuiDisplayNameAnnotation:     "Part 2 of layer",
			},
		},
		Spec: v1alpha1.ChunkSpec{
			Source: v1alpha1.ChunkHTTP{
				Request: v1alpha1.ChunkHTTPRequest{
					Method:  "GET",
					URL:     "http://source.example/blob",
					Headers: map[string]string{"Range": "bytes=10-19", "X-Token": "token"},
				},
				Response: v1alpha1.ChunkHTTPResponse{
					StatusCode: 206,
					Headers:    map[string]string{"Content-Range": "bytes 10-19/30"},
				},
			},
			BearerName: "bearer",
			Destination: []v1alpha1.ChunkHTTP{{
				Request:  v1alpha1.ChunkHTTPRequest{Method: "PUT", Headers: map[string]string{"Content-Length": "30"}},
				Response: v1alpha1.ChunkHTTPResponse{StatusCode: 200},
			}},
			Priority:                  7,
			Total:                     10,
			ChunkIndex:                2,
			ChunksNumber:              3,
			Sha256PartialPreviousName: "part-1",
			MaximumRetry:              2,
		},
		Status: v1alpha1.ChunkStatus{Phase: v1alpha1.ChunkPhasePending},
	}
	if !reflect.DeepEqual(chunk, want) {
		t.Errorf("chunk\n%+v\nwant\n%+v", chunk, want)
	}
}

func TestBuildChunkNeedsAClientAndAnUploadIDPerDestination(t *testing.T) {
	tests := []struct {
		name      string
		s3        map[string]*sss.SSS
		uploadIDs []string
	}{
		{name: "client missing", s3: map[string]*sss.SSS{}, uploadIDs: []string{"upload-1"}},
		{name: "upload ID empty", s3: newTestStorage(t), uploadIDs: []string{""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
			blob.Status.Total = 30
			c, _ := newTestToChunkController(t, tt.s3)

			if _, err := c.buildChunk(blob, "part-1", 1, 0, 10, "-", tt.uploadIDs); err == nil {
				t.Error("built a chunk, want an error")
			}
		})
	}
}
