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
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type testSource struct {
	status       int
	content      []byte
	acceptRanges bool
	noLength     bool
	// hungUp holds the body past pauseAfter back until the client hangs up.
	pauseAfter int
	hungUp     chan<- bool
}

func startSource(t *testing.T, source testSource) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if source.acceptRanges {
			w.Header().Set("Accept-Ranges", "bytes")
		}
		if !source.noLength {
			w.Header().Set("Content-Length", strconv.Itoa(len(source.content)))
		}
		w.WriteHeader(cmp.Or(source.status, http.StatusOK))
		if source.noLength {
			// Flushing before the body makes the server stream it without a length.
			w.(http.Flusher).Flush()
		}
		body := source.content
		if source.hungUp != nil {
			_, _ = w.Write(body[:source.pauseAfter])
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
			source.hungUp <- r.Context().Err() != nil
			body = body[source.pauseAfter:]
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

type testUpload struct {
	contentLength       int64
	transferEncoding    []string
	body                []byte
	contentLengthHeader string
}

type testDestination struct {
	url     string
	mut     sync.Mutex
	uploads []testUpload
}

func startDestination(t *testing.T, etag string, release <-chan struct{}) *testDestination {
	t.Helper()
	destination := &testDestination{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if release != nil {
			<-release
		}
		body, err := io.ReadAll(r.Body)
		destination.mut.Lock()
		destination.uploads = append(destination.uploads, testUpload{r.ContentLength, r.TransferEncoding, body, r.Header.Get("Content-Length")})
		destination.mut.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("ETag", strconv.Quote(etag))
	}))
	t.Cleanup(server.Close)
	destination.url = server.URL
	return destination
}

func (destination *testDestination) received() []testUpload {
	destination.mut.Lock()
	defer destination.mut.Unlock()
	return slices.Clone(destination.uploads)
}

func testContent(size int) []byte {
	content := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(content)
	return content
}

func newFirstPartChunk(source string, firstPart *v1alpha1.ChunkFirstPart, destinations ...*testDestination) *v1alpha1.Chunk {
	chunk := newTestChunk("blob:first:test", 0, 0)
	chunk.Status.HandlerName = "runner-test"
	chunk.Status.Phase = v1alpha1.ChunkPhaseRunning
	chunk.Spec.MaximumRetry = 3
	chunk.Spec.FirstPart = firstPart
	chunk.Spec.Source = v1alpha1.ChunkHTTP{
		Request:  v1alpha1.ChunkHTTPRequest{Method: http.MethodGet, URL: source},
		Response: v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK},
	}
	for _, destination := range destinations {
		chunk.Spec.Destination = append(chunk.Spec.Destination, v1alpha1.ChunkHTTP{
			Request:  v1alpha1.ChunkHTTPRequest{Method: http.MethodPut, URL: destination.url},
			Response: v1alpha1.ChunkHTTPResponse{StatusCode: http.StatusOK},
		})
	}
	return chunk
}

func newProcessRunner(t *testing.T, chunk *v1alpha1.Chunk) (*ChunkRunner, *fake.Clientset) {
	t.Helper()
	runner, client := newTestRunner(t, 1, []*v1alpha1.Chunk{chunk}, nil)
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	runner.httpClient = &http.Client{Transport: transport}
	return runner, client
}

func runProcess(runner *ChunkRunner, chunk *v1alpha1.Chunk) {
	continues := make(chan struct{})
	close(continues)
	runner.process(continues, chunk.DeepCopy())
}

func waitForResult(t *testing.T, client *fake.Clientset, name string) *v1alpha1.Chunk {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		chunk, err := client.TaskV1alpha1().Chunks().Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Status.Phase == v1alpha1.ChunkPhaseSucceeded || chunk.Status.Phase == v1alpha1.ChunkPhaseFailed {
			return chunk
		}
		if time.Now().After(deadline) {
			t.Fatalf("chunk %s reported no result within 5s: phase=%s", name, chunk.Status.Phase)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func processChunk(t *testing.T, chunk *v1alpha1.Chunk) *v1alpha1.Chunk {
	t.Helper()
	runner, client := newProcessRunner(t, chunk)
	runProcess(runner, chunk)
	return waitForResult(t, client, chunk.Name)
}

func expectUploads(t *testing.T, content []byte, destinations ...*testDestination) {
	t.Helper()
	for index, destination := range destinations {
		uploads := destination.received()
		if len(uploads) != 1 {
			t.Errorf("destination %d received %d uploads, want 1", index, len(uploads))
			continue
		}
		got := uploads[0]
		if got.contentLength != int64(len(content)) || len(got.transferEncoding) != 0 || !bytes.Equal(got.body, content) {
			t.Errorf("destination %d received %d bytes with Content-Length %d and transfer encoding %v, want the expected %d bytes with that length",
				index, len(got.body), got.contentLength, got.transferEncoding, len(content))
		}
	}
}

func TestFirstPartTakesTheWholeBodyWhenTheLayoutIsOneChunk(t *testing.T) {
	content := testContent(70000)
	for _, test := range []struct {
		name      string
		source    testSource
		firstPart v1alpha1.ChunkFirstPart
	}{
		{"content within one chunk", testSource{content: content, acceptRanges: true}, v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20}},
		{"source without ranges, not forced", testSource{content: content}, v1alpha1.ChunkFirstPart{ChunkSize: 16384}},
		{"response without content length", testSource{content: content, acceptRanges: true, noLength: true}, v1alpha1.ChunkFirstPart{ChunkSize: 16384}},
	} {
		for _, withFirstPart := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/firstPart=%t", test.name, withFirstPart), func(t *testing.T) {
				source := startSource(t, test.source)
				destinations := []*testDestination{startDestination(t, "etag-0", nil), startDestination(t, "etag-1", nil)}
				chunk := newFirstPartChunk(source, &test.firstPart, destinations...)
				if !withFirstPart {
					chunk.Spec.FirstPart = nil
				}
				result := processChunk(t, chunk)
				status := result.Status
				if status.Phase != v1alpha1.ChunkPhaseSucceeded || status.SourceProgress != int64(len(content)) || !reflect.DeepEqual(status.Etags, []string{"etag-0", "etag-1"}) {
					t.Errorf("got phase=%s sourceProgress=%d etags=%v conditions=%v, want Succeeded, %d and both etags in destination order",
						status.Phase, status.SourceProgress, status.Etags, status.Conditions, len(content))
				}
				expectUploads(t, content, destinations...)
			})
		}
	}
}

func TestFirstPartTakesOnlyPartOneWhenTheLayoutHasMoreChunks(t *testing.T) {
	for _, test := range []struct {
		name         string
		size         int
		acceptRanges bool
		firstPart    v1alpha1.ChunkFirstPart
		part         int
	}{
		{"minimum chunk size", 16 << 20, true, v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20}, 5592406},
		{"source without ranges, forced", 70000, false, v1alpha1.ChunkFirstPart{ChunkSize: 16384, ForceAcceptRanges: true}, 16384},
		{"chunk size only", 70000, true, v1alpha1.ChunkFirstPart{ChunkSize: 30000}, 30000},
		{"chunks number only", 6<<20 + 1, true, v1alpha1.ChunkFirstPart{ChunksNumber: 4}, 1572865},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := testContent(test.size)
			hungUp := make(chan bool, 1)
			source := startSource(t, testSource{content: content, acceptRanges: test.acceptRanges, pauseAfter: test.part, hungUp: hungUp})
			destinations := []*testDestination{startDestination(t, "etag-0", nil), startDestination(t, "etag-1", nil)}
			status := processChunk(t, newFirstPartChunk(source, &test.firstPart, destinations...)).Status
			if status.Phase != v1alpha1.ChunkPhaseSucceeded || status.SourceProgress != int64(test.part) || !reflect.DeepEqual(status.Etags, []string{"etag-0", "etag-1"}) {
				t.Errorf("got phase=%s sourceProgress=%d etags=%v conditions=%v, want Succeeded, %d and both etags in destination order",
					status.Phase, status.SourceProgress, status.Etags, status.Conditions, test.part)
			}
			expectUploads(t, content[:test.part], destinations...)
			select {
			case cut := <-hungUp:
				if !cut {
					t.Error("the runner read the source past part 1 instead of hanging up")
				}
			case <-time.After(5 * time.Second):
				t.Error("the source saw no hang-up within 5s after the chunk finished")
			}
		})
	}
}

func TestFirstPartReleasesTheSourceBeforeTheUploadEnds(t *testing.T) {
	content := testContent(70000)
	hungUp := make(chan bool, 1)
	source := startSource(t, testSource{content: content, acceptRanges: true, pauseAfter: 30000, hungUp: hungUp})
	release := make(chan struct{})
	destination := startDestination(t, "etag-0", release)
	releaseDestination := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseDestination)
	chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{ChunkSize: 30000}, destination)
	runner, client := newProcessRunner(t, chunk)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProcess(runner, chunk)
	}()
	t.Cleanup(func() {
		releaseDestination()
		<-done
	})
	select {
	case cut := <-hungUp:
		if !cut {
			t.Error("the runner read the source past part 1 instead of hanging up")
		}
	case <-time.After(5 * time.Second):
		t.Error("the source saw no hang-up within 5s while the upload of part 1 was blocked")
	}
	select {
	case <-done:
		t.Fatal("the chunk finished although its destination was still blocked")
	default:
	}
	releaseDestination()
	<-done
	status := waitForResult(t, client, chunk.Name).Status
	if status.Phase != v1alpha1.ChunkPhaseSucceeded || status.SourceProgress != 30000 {
		t.Errorf("got phase=%s sourceProgress=%d conditions=%v after the destination answered, want Succeeded and 30000",
			status.Phase, status.SourceProgress, status.Conditions)
	}
	expectUploads(t, content[:30000], destination)
}

func TestFirstPartLeavesTheHashStateOfPartOneForTheNextPart(t *testing.T) {
	content := testContent(16 << 20)
	whole := sha256.Sum256(content)
	source := startSource(t, testSource{content: content, acceptRanges: true})
	chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20}, startDestination(t, "etag-0", nil))
	chunk.Spec.Sha256PartialPreviousName = "-"
	chunk.Spec.Sha256 = hex.EncodeToString(whole[:])
	status := processChunk(t, chunk).Status
	if status.Phase != v1alpha1.ChunkPhaseSucceeded {
		t.Fatalf("got phase=%s conditions=%v, want Succeeded although Spec.Sha256 is the digest of the whole content", status.Phase, status.Conditions)
	}
	resumed := sha256.New()
	if err := resumed.(encoding.BinaryUnmarshaler).UnmarshalBinary(status.Sha256Partial); err != nil {
		t.Fatalf("sha256Partial of %d bytes is no sha256 state: %v", len(status.Sha256Partial), err)
	}
	resumed.Write(content[5592406:])
	if got := resumed.Sum(nil); !bytes.Equal(got, whole[:]) {
		t.Errorf("the state continued with the remaining bytes gives %x, want the digest of the whole content %x", got, whole)
	}
}

func TestFirstPartWholeBodyIsCheckedAgainstSha256(t *testing.T) {
	content := testContent(70000)
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	for _, test := range []struct {
		name   string
		sha256 string
	}{
		{"matching", digest},
		{"mismatching", strings.Repeat("0", 64)},
	} {
		for _, withFirstPart := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/firstPart=%t", test.name, withFirstPart), func(t *testing.T) {
				source := startSource(t, testSource{content: content, acceptRanges: true})
				chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20}, startDestination(t, "etag-0", nil))
				if !withFirstPart {
					chunk.Spec.FirstPart = nil
				}
				chunk.Spec.Sha256PartialPreviousName = "-"
				chunk.Spec.Sha256 = test.sha256
				status := processChunk(t, chunk).Status
				if test.sha256 == digest {
					if status.Phase != v1alpha1.ChunkPhaseSucceeded || status.Sha256 != digest {
						t.Errorf("got phase=%s sha256=%q conditions=%v, want Succeeded with sha256 %q", status.Phase, status.Sha256, status.Conditions, digest)
					}
					return
				}
				condition, _ := v1alpha1.GetCondition(status.Conditions, "Sha256UpdateError")
				if status.Phase != v1alpha1.ChunkPhaseFailed || status.Retryable || !strings.Contains(condition.Message, "sha256 mismatch") {
					t.Errorf("got phase=%s retryable=%t conditions=%v, want Failed, not retryable, with a sha256 mismatch condition", status.Phase, status.Retryable, status.Conditions)
				}
			})
		}
	}
}

func TestFirstPartSourceErrorsFailLikeChunksWithoutFirstPart(t *testing.T) {
	body := []byte("denied")
	for _, test := range []struct {
		name   string
		status int
		bearer string
	}{
		{"401", http.StatusUnauthorized, ""},
		{"401 with bearer", http.StatusUnauthorized, "unresolved"},
		{"404", http.StatusNotFound, ""},
		{"500", http.StatusInternalServerError, ""},
		{"503", http.StatusServiceUnavailable, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := startSource(t, testSource{status: test.status, content: body, acceptRanges: true})
			var results []v1alpha1.ChunkStatus
			// The size matches the error body, so a skip decided before the status check would succeed.
			for _, firstPart := range []*v1alpha1.ChunkFirstPart{nil, {MinimumChunkSize: 5 << 20, ExistingSizes: []int64{int64(len(body))}}} {
				chunk := newFirstPartChunk(source, firstPart, startDestination(t, "etag-0", nil))
				chunk.Spec.BearerName = test.bearer
				results = append(results, processChunk(t, chunk).Status)
			}
			plain, first := results[0], results[1]
			if first.Phase != v1alpha1.ChunkPhaseFailed {
				t.Fatalf("got phase=%s conditions=%v, want Failed", first.Phase, first.Conditions)
			}
			if first.Retryable != plain.Retryable || !reflect.DeepEqual(first.Conditions, plain.Conditions) || responseCode(first) != responseCode(plain) {
				t.Errorf("got retryable=%t conditions=%v response=%d, want retryable=%t conditions=%v response=%d as without firstPart",
					first.Retryable, first.Conditions, responseCode(first), plain.Retryable, plain.Conditions, responseCode(plain))
			}
		})
	}
}

func responseCode(status v1alpha1.ChunkStatus) int {
	if status.SourceResponse == nil {
		return 0
	}
	return status.SourceResponse.StatusCode
}

func TestFirstPartReportsSourceResponseBeforeTheTransferEnds(t *testing.T) {
	content := testContent(70000)
	source := startSource(t, testSource{content: content, acceptRanges: true})
	release := make(chan struct{})
	destination := startDestination(t, "etag-0", release)
	releaseDestination := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseDestination)
	chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20}, destination)
	runner, client := newProcessRunner(t, chunk)
	runner.updateDuration = 10 * time.Millisecond
	updates := trackStatusUpdates(client, "chunks", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProcess(runner, chunk)
	}()
	t.Cleanup(func() {
		releaseDestination()
		<-done
	})
	reported := func() bool {
		updates.mut.Lock()
		defer updates.mut.Unlock()
		for _, object := range updates.objects {
			status := object.(*v1alpha1.Chunk).Status
			if status.Phase == v1alpha1.ChunkPhaseRunning && status.SourceResponse != nil &&
				status.SourceResponse.StatusCode == http.StatusOK && status.SourceResponse.Headers["content-length"] == "70000" {
				return true
			}
		}
		return false
	}
	deadline := time.Now().Add(5 * time.Second)
	for !reported() {
		if time.Now().After(deadline) {
			t.Fatal("no status update carried the source response while the destination was blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	releaseDestination()
	<-done
	if status := waitForResult(t, client, chunk.Name).Status; status.Phase != v1alpha1.ChunkPhaseSucceeded {
		t.Errorf("got phase=%s conditions=%v after the destination answered, want Succeeded", status.Phase, status.Conditions)
	}
}

func TestFirstPartSkipsWhenEveryDestinationHoldsTheTotal(t *testing.T) {
	content := testContent(70000)
	hungUp := make(chan bool, 1)
	// The whole body is held back, so only the headers reach the runner.
	source := startSource(t, testSource{content: content, acceptRanges: true, hungUp: hungUp})
	destinations := []*testDestination{startDestination(t, "etag-0", nil), startDestination(t, "etag-1", nil)}
	chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{ChunkSize: 16384, ExistingSizes: []int64{70000, 70000}}, destinations...)
	// Progress left behind by an earlier attempt.
	chunk.Status.Retry = 1
	chunk.Status.SourceProgress = 4096
	status := processChunk(t, chunk).Status
	if status.Phase != v1alpha1.ChunkPhaseSucceeded || len(status.Etags) != 0 || status.SourceProgress != 0 {
		t.Errorf("got phase=%s etags=%v sourceProgress=%d conditions=%v, want Succeeded without etags and sourceProgress 0",
			status.Phase, status.Etags, status.SourceProgress, status.Conditions)
	}
	for index, destination := range destinations {
		if uploads := destination.received(); len(uploads) != 0 {
			t.Errorf("destination %d received %d requests, want none", index, len(uploads))
		}
	}
	select {
	case cut := <-hungUp:
		if !cut {
			t.Error("the runner read the source body instead of hanging up")
		}
	case <-time.After(5 * time.Second):
		t.Error("the source saw no hang-up within 5s after the skip, so its connection is still held")
	}
}

func TestFirstPartTransfersToEveryDestinationUnlessAllHoldTheTotal(t *testing.T) {
	content := testContent(70000)
	for _, test := range []struct {
		name     string
		noLength bool
		sizes    []int64
	}{
		{"one size differs", false, []int64{70000, 69999}},
		{"one destination has nothing to compare", false, []int64{70000, 0}},
		{"empty size list", false, []int64{}},
		{"unknown total", true, []int64{70000, 70000}},
		{"zero sizes and unknown total", true, []int64{0, 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := startSource(t, testSource{content: content, acceptRanges: true, noLength: test.noLength})
			destinations := []*testDestination{startDestination(t, "etag-0", nil), startDestination(t, "etag-1", nil)}
			chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20, ExistingSizes: test.sizes}, destinations...)
			status := processChunk(t, chunk).Status
			if status.Phase != v1alpha1.ChunkPhaseSucceeded || !reflect.DeepEqual(status.Etags, []string{"etag-0", "etag-1"}) {
				t.Errorf("got phase=%s etags=%v conditions=%v, want Succeeded with both etags", status.Phase, status.Etags, status.Conditions)
			}
			expectUploads(t, content, destinations...)
		})
	}
}

func TestEmptyContentIsUploadedWithContentLengthZero(t *testing.T) {
	for _, test := range []struct {
		name   string
		source testSource
	}{
		{"response with content length 0", testSource{acceptRanges: true}},
		{"response without content length", testSource{acceptRanges: true, noLength: true}},
	} {
		for _, withFirstPart := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/firstPart=%t", test.name, withFirstPart), func(t *testing.T) {
				source := startSource(t, test.source)
				destinations := []*testDestination{startDestination(t, "etag-0", nil), startDestination(t, "etag-1", nil)}
				chunk := newFirstPartChunk(source, &v1alpha1.ChunkFirstPart{MinimumChunkSize: 5 << 20}, destinations...)
				if !withFirstPart {
					chunk.Spec.FirstPart = nil
				}
				status := processChunk(t, chunk).Status
				if status.Phase != v1alpha1.ChunkPhaseSucceeded || !reflect.DeepEqual(status.Etags, []string{"etag-0", "etag-1"}) {
					t.Errorf("got phase=%s etags=%v conditions=%v, want Succeeded with both etags in destination order",
						status.Phase, status.Etags, status.Conditions)
				}
				expectUploads(t, nil, destinations...)
				for index, destination := range destinations {
					for _, upload := range destination.received() {
						if upload.contentLengthHeader != "0" {
							t.Errorf("destination %d received the header Content-Length %q, want \"0\"", index, upload.contentLengthHeader)
						}
					}
				}
			})
		}
	}
}
