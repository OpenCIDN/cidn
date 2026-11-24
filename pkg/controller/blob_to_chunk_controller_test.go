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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/wzshiming/sss"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
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

func newTestMultipart(blob *v1alpha1.Blob, parts int64, uploadIDs ...string) *v1alpha1.Multipart {
	return &v1alpha1.Multipart{
		ObjectMeta:       metav1.ObjectMeta{Name: blob.Name},
		DestinationNames: []string{"storage"},
		UploadIDs:        uploadIDs,
		UploadEtags:      make([]v1alpha1.UploadEtags, parts),
	}
}

func multipartExists(t *testing.T, client *fake.Clientset, name string) bool {
	t.Helper()
	_, err := client.TaskV1alpha1().Multiparts().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

func TestCleanupBlobRemovesTheChunksAndOnlyAMultipartBlobsUpload(t *testing.T) {
	tests := []struct {
		name          string
		skipHead      bool
		chunksNumber  int64
		wantMultipart bool
	}{
		{name: "more than one chunk", chunksNumber: 3},
		{name: "one chunk", chunksNumber: 1, wantMultipart: true},
		{name: "skipHead with one chunk", skipHead: true, chunksNumber: 1},
		{name: "skipHead before its layout", skipHead: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.SkipHead = tt.skipHead
			blob.Spec.ChunksNumber = tt.chunksNumber
			chunk := newTestChunk("chunk", blob, v1alpha1.ChunkPhaseRunning)
			c, client := newTestToChunkController(t, map[string]*sss.SSS{}, blob, chunk, newTestMultipart(blob, tt.chunksNumber, "upload-1"))

			c.cleanupBlob(blob)

			if _, err := client.TaskV1alpha1().Chunks().Get(context.Background(), chunk.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("chunk still there: %v", err)
			}
			if got := multipartExists(t, client, blob.Name); got != tt.wantMultipart {
				t.Errorf("multipart kept %t, want %t", got, tt.wantMultipart)
			}
		})
	}
}

func TestBlobWithoutSkipHeadStartsWithAHeadOfItsFirstSource(t *testing.T) {
	blob := newTestBlob()
	blob.Spec.Source = []v1alpha1.BlobSource{
		{URL: "http://source.example/blob", BearerName: "bearer", Headers: map[string]string{"x-token": "token"}},
		{URL: "http://mirror.example/blob"},
	}
	blob.Spec.Priority = 7
	blob.Status.Retry = 1
	c, client := newTestToChunkController(t, map[string]*sss.SSS{}, blob)

	c.handler(context.Background(), blob.Name)

	chunk, err := client.TaskV1alpha1().Chunks().Get(context.Background(), "blob:head:blob:0", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(chunk.Annotations, map[string]string{BlobNameAnnotationKey: "blob"}) {
		t.Errorf("annotations %v, want only the blob name", chunk.Annotations)
	}
	wantSpec := v1alpha1.ChunkSpec{
		Source: v1alpha1.ChunkHTTP{
			Request:  v1alpha1.ChunkHTTPRequest{Method: "HEAD", URL: "http://source.example/blob", Headers: map[string]string{"X-Token": "token"}},
			Response: v1alpha1.ChunkHTTPResponse{StatusCode: 200},
		},
		BearerName:   "bearer",
		Priority:     7,
		MaximumRetry: 2,
	}
	if !reflect.DeepEqual(chunk.Spec, wantSpec) {
		t.Errorf("spec\n%+v\nwant\n%+v", chunk.Spec, wantSpec)
	}
}

func chunkNames(t *testing.T, client *fake.Clientset) []string {
	t.Helper()
	list, err := client.TaskV1alpha1().Chunks().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, chunk := range list.Items {
		names = append(names, chunk.Name)
	}
	return names
}

func TestSkipHeadBlobStartsWithItsFirstChunkOnly(t *testing.T) {
	for _, chunksNumber := range []int64{0, 1, 4} {
		t.Run(fmt.Sprintf("preset chunks number %d", chunksNumber), func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.SkipHead = true
			blob.Spec.ChunksNumber = chunksNumber
			c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestMultipart(blob, 1, "upload-1"))

			c.handler(context.Background(), blob.Name)

			if got, want := chunkNames(t, client), []string{"blob:first:blob"}; !reflect.DeepEqual(got, want) {
				t.Errorf("chunks %v, want %v", got, want)
			}
		})
	}
}

func TestExistingFirstChunkNeedsNothingFromTheStore(t *testing.T) {
	blob := newTestSkipHeadBlob()
	c, _ := newTestToChunkController(t, map[string]*sss.SSS{}, blob, newTestFirstChunk(blob, v1alpha1.ChunkPhaseFailed))

	if err := c.toFirstChunk(context.Background(), blob); err != nil {
		t.Errorf("toFirstChunk: %v, want nothing to do", err)
	}
}

// newTestUploadStorage is a store without unfinished uploads that opens the next one as uploadID, accepts any commit, and lists the requests it got.
func newTestUploadStorage(t *testing.T, uploadID string) (map[string]*sss.SSS, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("uploads"):
			io.WriteString(w, "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>")
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			fmt.Fprintf(w, "<InitiateMultipartUploadResult><Key>blob</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>", uploadID)
		case r.Method == http.MethodPost && r.URL.Query().Has("uploadId"):
			io.WriteString(w, "<CompleteMultipartUploadResult><Key>blob</Key></CompleteMultipartUploadResult>")
		default:
			t.Errorf("unexpected store call %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)
	s, err := sss.NewSSS(sss.WithAccessKey("key"), sss.WithSecretKey("secret"), sss.WithBucket("bucket"), sss.WithRegion("us-east-1"), sss.WithRegionEndpoint(srv.URL), sss.WithForcePathStyle(true))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*sss.SSS{"storage": s}, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(requests)
	}
}

func TestSkipHeadBlobStartsWithAFreshMultipartInsteadOfAStaleOne(t *testing.T) {
	tests := []struct {
		name         string
		slots        []v1alpha1.UploadEtags
		wantUploadID string
	}{
		{name: "fresh one-slot record", slots: make([]v1alpha1.UploadEtags, 1), wantUploadID: "upload-1"},
		{name: "no slot", slots: []v1alpha1.UploadEtags{}, wantUploadID: "upload-2"},
		{name: "more slots", slots: make([]v1alpha1.UploadEtags, 3), wantUploadID: "upload-2"},
		{name: "recorded part 1", slots: []v1alpha1.UploadEtags{{Size: 10, Etags: []string{"old-etag-1"}}}, wantUploadID: "upload-2"},
		{name: "part 1 with a size only", slots: []v1alpha1.UploadEtags{{Size: 10}}, wantUploadID: "upload-2"},
		{name: "recorded part 2", slots: []v1alpha1.UploadEtags{{}, {Size: 10, Etags: []string{"old-etag-2"}}}, wantUploadID: "upload-2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			mp := newTestMultipart(blob, 0, "upload-1")
			mp.UploadEtags = tt.slots
			stores, _ := newTestUploadStorage(t, "upload-2")
			c, client := newTestToChunkController(t, stores, blob, mp)

			c.handler(context.Background(), blob.Name)

			got, err := client.TaskV1alpha1().Multiparts().Get(context.Background(), blob.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.UploadIDs, []string{tt.wantUploadID}) || !reflect.DeepEqual(got.UploadEtags, make([]v1alpha1.UploadEtags, 1)) {
				t.Errorf("upload IDs %v, parts %+v; want [%s] and one empty part", got.UploadIDs, got.UploadEtags, tt.wantUploadID)
			}
			checkPartURLs(t, getChunk(t, client, "blob:first:blob"), 1, tt.wantUploadID)
		})
	}
}

func TestFirstChunkGetsTheFirstSourceIntoPartOne(t *testing.T) {
	blob := newTestBlob()
	blob.Annotations = map[string]string{v1alpha1.WebuiGroupAnnotation: "group", v1alpha1.WebuiDisplayNameAnnotation: "layer"}
	blob.Spec.Source = []v1alpha1.BlobSource{
		{URL: "http://source.example/blob", BearerName: "bearer", Headers: map[string]string{"x-token": "token"}},
		{URL: "http://mirror.example/blob"},
	}
	blob.Spec.SkipHead = true
	blob.Spec.Priority = 7
	blob.Spec.ContentSha256 = "sha"
	blob.Spec.MinimumChunkSize, blob.Spec.ChunkSize, blob.Spec.ChunksNumber, blob.Spec.ForceAcceptRanges = 5<<20, 6<<20, 4, true
	blob.Status.Retry = 1
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	chunk, err := client.TaskV1alpha1().Chunks().Get(context.Background(), "blob:first:blob", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	checkPartURLs(t, chunk, 1, "upload-1")
	wantAnnotations := map[string]string{
		BlobNameAnnotationKey:                   "blob",
		v1alpha1.WebuiGroupAnnotation:           "group",
		v1alpha1.WebuiGroupIgnoreSizeAnnotation: "true",
		v1alpha1.WebuiDisplayNameAnnotation:     "Part 1 of layer",
	}
	if !reflect.DeepEqual(chunk.Annotations, wantAnnotations) {
		t.Errorf("annotations %v, want %v", chunk.Annotations, wantAnnotations)
	}
	wantSpec := v1alpha1.ChunkSpec{
		Source: v1alpha1.ChunkHTTP{
			Request:  v1alpha1.ChunkHTTPRequest{Method: "GET", URL: "http://source.example/blob", Headers: map[string]string{"Accept-Encoding": "identity", "X-Token": "token"}},
			Response: v1alpha1.ChunkHTTPResponse{StatusCode: 200},
		},
		BearerName: "bearer",
		Destination: []v1alpha1.ChunkHTTP{{
			Request:  v1alpha1.ChunkHTTPRequest{Method: "PUT", Headers: map[string]string{}},
			Response: v1alpha1.ChunkHTTPResponse{StatusCode: 200},
		}},
		Priority:                  7,
		ChunkIndex:                1,
		Sha256PartialPreviousName: "-",
		Sha256:                    "sha",
		MaximumRetry:              2,
		FirstPart:                 &v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20, ChunkSize: 6 << 20, ChunksNumber: 4, ForceAcceptRanges: true},
	}
	if !reflect.DeepEqual(chunk.Spec, wantSpec) {
		t.Errorf("spec\n%+v\nwant\n%+v", chunk.Spec, wantSpec)
	}
}

func TestOnlyTheFirstChunkAsksForTheIdentityEncoding(t *testing.T) {
	tests := []struct {
		name         string
		skipHead     bool
		chunksNumber int64
		total        int64
		headers      map[string]string
		chunkName    string
		want         string
	}{
		{name: "first chunk", skipHead: true, chunkName: "blob:first:blob", want: "identity"},
		{name: "first chunk of a source that sets it", skipHead: true, headers: map[string]string{"accept-encoding": "gzip"}, chunkName: "blob:first:blob", want: "gzip"},
		{name: "HEAD chunk", chunkName: "blob:head:blob:0"},
		{name: "full chunk", chunksNumber: 1, total: 1024, chunkName: "blob:full:blob"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestBlob()
			blob.Spec.Source[0].Headers = tt.headers
			blob.Spec.SkipHead = tt.skipHead
			blob.Spec.ChunksNumber, blob.Status.Total = tt.chunksNumber, tt.total
			c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestMultipart(blob, 1, "upload-1"))

			c.handler(context.Background(), blob.Name)

			if got := getChunk(t, client, tt.chunkName).Spec.Source.Request.Headers["Accept-Encoding"]; got != tt.want {
				t.Errorf("Accept-Encoding %q, want %q", got, tt.want)
			}
		})
	}
}

// waitForChunkNames waits for the chunks createChunks creates in the background, then for no others.
func waitForChunkNames(t *testing.T, client *fake.Clientset, want ...string) {
	t.Helper()
	var got []string
	if err := wait.PollUntilContextTimeout(context.Background(), 5*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		got = chunkNames(t, client)
		return reflect.DeepEqual(got, want), nil
	}); err != nil {
		t.Fatalf("chunks %v, want %v", got, want)
	}
	time.Sleep(100 * time.Millisecond)
	if got = chunkNames(t, client); !reflect.DeepEqual(got, want) {
		t.Fatalf("chunks %v, want only %v", got, want)
	}
}

func TestLaidOutBlobWithoutSkipHeadCreatesItsPartsFromPartOne(t *testing.T) {
	blob := newTestBlob()
	blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
	blob.Spec.MaximumRunning, blob.Spec.MaximumPending = 3, 2
	blob.Status.Total, blob.Status.AcceptRanges = 30, true
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestMultipart(blob, 3, "upload-1"))

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:part:blob:1:00-0a", "blob:part:blob:2:0a-14")
}

func getChunk(t *testing.T, client *fake.Clientset, name string) *v1alpha1.Chunk {
	t.Helper()
	chunk, err := client.TaskV1alpha1().Chunks().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}

func newTestLaidOutSkipHeadBlob() *v1alpha1.Blob {
	blob := newTestSkipHeadBlob()
	blob.Spec.ChunksNumber, blob.Spec.ChunkSize = 3, 10
	blob.Status.Total, blob.Status.AcceptRanges = 30, true
	return blob
}

func TestLaidOutSkipHeadBlobCreatesItsOtherPartsLikeAnyBlob(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	blob.Spec.MaximumRunning, blob.Spec.MaximumPending = 3, 2
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestFirstChunk(blob, v1alpha1.ChunkPhaseRunning), newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:first:blob", "blob:part:blob:2:0a-14", "blob:part:blob:3:14-1e")
}

func TestPartTwoOfASkipHeadBlobContinuesTheHashOfItsFirstChunk(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	blob.Spec.ContentSha256 = "sha"
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestFirstChunk(blob, v1alpha1.ChunkPhaseRunning), newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:first:blob", "blob:part:blob:2:0a-14")
	if got := getChunk(t, client, "blob:part:blob:2:0a-14").Spec.Sha256PartialPreviousName; got != "blob:first:blob" {
		t.Errorf("part 2 continues the hash of %q, want blob:first:blob", got)
	}
}

func TestRunningFirstChunkCountsAgainstMaximumRunning(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	blob.Spec.MaximumRunning, blob.Spec.MaximumPending = 2, 2
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestFirstChunk(blob, v1alpha1.ChunkPhaseRunning), newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:first:blob", "blob:part:blob:2:0a-14")
}

func TestRecordedPartOneGetsNoFirstChunkAgain(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	mp := newTestMultipart(blob, 3, "upload-1")
	mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	c, client := newTestToChunkController(t, newTestStorage(t), blob, mp)

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:part:blob:2:0a-14")
}

func TestRecordedFirstChunkGoesOnceItsNextPartIsRecorded(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	mp := newTestMultipart(blob, 3, "upload-1")
	mp.UploadEtags[0] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-1"}}
	mp.UploadEtags[1] = v1alpha1.UploadEtags{Size: 10, Etags: []string{"etag-2"}}
	partTwo := newTestChunk("blob:part:blob:2:0a-14", blob, v1alpha1.ChunkPhaseSucceeded)
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestSucceededFirstChunk(blob, 10, "etag-1"), partTwo, mp)

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:part:blob:2:0a-14", "blob:part:blob:3:14-1e")
}

func TestSkippedFirstChunkLeavesTheOtherPartsUncreated(t *testing.T) {
	blob := newTestLaidOutSkipHeadBlob()
	c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestSucceededFirstChunk(blob, 0), newTestMultipart(blob, 1, "upload-1"))

	c.handler(context.Background(), blob.Name)

	waitForChunkNames(t, client, "blob:first:blob")
}

func TestFirstChunkCreatedAfterTheLayoutLeadsTheRunnerToIt(t *testing.T) {
	ranged := func(total int64) map[string]string {
		return map[string]string{"content-length": strconv.FormatInt(total, 10), "accept-ranges": "bytes"}
	}
	tests := []struct {
		name              string
		headers           map[string]string
		minimumChunkSize  int64
		chunkSize         int64
		chunksNumber      int64
		forceAcceptRanges bool
	}{
		{name: "MinimumChunkSize", headers: ranged(16 << 20), minimumChunkSize: 5 << 20},
		{name: "ChunkSize only", headers: ranged(16 << 20), chunkSize: 6 << 20},
		{name: "ChunksNumber only", headers: ranged(64 << 20), chunksNumber: 4},
		{name: "MinimumChunkSize over ChunksNumber 1", headers: ranged(16 << 20), minimumChunkSize: 5 << 20, chunksNumber: 1},
		{name: "ForceAcceptRanges", headers: map[string]string{"content-length": "16777216"}, minimumChunkSize: 5 << 20, forceAcceptRanges: true},
		{name: "both presets", headers: ranged(16 << 20), chunkSize: 6 << 20, chunksNumber: 4},
		{name: "ChunkSize above the total", headers: ranged(16 << 20), chunkSize: 20 << 20},
		{name: "one chunk capped at 5 GiB", headers: ranged(8 << 30), minimumChunkSize: 10 << 30},
		{name: "no range support", headers: map[string]string{"content-length": "16777216"}, minimumChunkSize: 5 << 20},
		{name: "unknown total", headers: map[string]string{}, minimumChunkSize: 5 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := newTestSkipHeadBlob()
			blob.Spec.MinimumChunkSize, blob.Spec.ForceAcceptRanges = tt.minimumChunkSize, tt.forceAcceptRanges
			// As the API server lays the blob out on the write that sets its total.
			blob.Status.Total, blob.Status.AcceptRanges = v1alpha1.ParseResponseHeaders(tt.headers, tt.forceAcceptRanges)
			blob.Spec.ChunksNumber, blob.Spec.ChunkSize = v1alpha1.ComputeChunkLayout(blob.Status.Total, blob.Status.AcceptRanges, tt.minimumChunkSize, tt.chunkSize, tt.chunksNumber)
			c, client := newTestToChunkController(t, newTestStorage(t), blob, newTestMultipart(blob, 1, "upload-1"))

			c.handler(context.Background(), blob.Name)

			waitForChunkNames(t, client, "blob:first:blob")
			firstPart := getChunk(t, client, "blob:first:blob").Spec.FirstPart
			// As the runner lays the blob out when the response headers arrive.
			total, acceptRanges := v1alpha1.ParseResponseHeaders(tt.headers, firstPart.ForceAcceptRanges)
			chunksNumber, chunkSize := v1alpha1.ComputeChunkLayout(total, acceptRanges, firstPart.MinimumChunkSize, firstPart.ChunkSize, firstPart.ChunksNumber)
			if chunksNumber != blob.Spec.ChunksNumber || (chunksNumber > 1 && chunkSize != blob.Spec.ChunkSize) {
				t.Errorf("runner lays out %d chunks of %d bytes, blob %d chunks of %d bytes", chunksNumber, chunkSize, blob.Spec.ChunksNumber, blob.Spec.ChunkSize)
			}
		})
	}
}
