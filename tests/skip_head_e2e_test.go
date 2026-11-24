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

package tests

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	taskv1alpha1 "github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// The controller ties every Chunk it creates to its Blob with this annotation.
	blobNameAnnotation = taskv1alpha1.GroupName + "/blob-name"
	// Time for the watch to deliver what was created just before the blob finished.
	chunkWatchSettle = 5 * time.Second
)

// Query arguments that choose nginx-test's behaviour; they combine.
const (
	// Answers every HEAD with 401, also with a valid token.
	rejectHead = "head=reject"
	// Ignores Range and sends no Accept-Ranges.
	noRange = "range=off"
	// Sends the body at 1 KiB/s.
	slowBody = "rate=slow"
)

func TestBlobWithoutSkipHeadFailsWhenTheSourceRejectsHead(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-head-rejected")
	sha := writeSource(t, name, 1<<20)
	createBlob(t, client, name, sha, sourceWith(name, rejectHead))

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseFailed {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseFailed, statusJSON(blob))
	}
	for _, condition := range blob.Status.Conditions {
		if condition.Type == "UnexpectedStatusCode" && strings.Contains(condition.Message, "got 401") {
			return
		}
	}
	t.Errorf("blob %s failed without an UnexpectedStatusCode condition for 401; status: %s", name, statusJSON(blob))
}

func TestSkipHeadBlobMultiChunkSucceedsWithoutHeadOrFullChunk(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-multi")
	sha := writeSource(t, name, 16<<20)
	countCreations := watchCreations(t, client, name)
	createBlob(t, client, name, sha, sourceWith(name, rejectHead), skipHead)

	blob := waitForBlobDone(t, client, name)
	created := countCreations()
	names := slices.Sorted(maps.Keys(created.chunks))
	t.Logf("chunks of blob %s: %v", name, names)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 3 {
		t.Errorf("blob %s was split into %d chunks, want 3", name, blob.Spec.ChunksNumber)
	}
	// The first chunk is part 1.
	want := []string{"blob:first:" + name, partChunkName(blob, 2), partChunkName(blob, 3)}
	if !slices.Equal(names, want) {
		t.Errorf("blob %s had chunks %v, want %v", name, names, want)
	}
	checkCreatedOnce(t, name, created)
}

func TestSkipHeadBlobSingleChunkSucceeds(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-single")
	sha := writeSource(t, name, 1<<20)
	countCreations := watchCreations(t, client, name)
	createBlob(t, client, name, sha, sourceWith(name, rejectHead), skipHead)

	blob := waitForBlobDone(t, client, name)
	created := countCreations()
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 1 {
		t.Errorf("blob %s was split into %d chunks, want 1", name, blob.Spec.ChunksNumber)
	}
	checkCreatedOnce(t, name, created)
}

func TestSkipHeadBlobWithoutRangeSupportSucceedsInOneChunk(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-no-range")
	sha := writeSource(t, name, 16<<20)
	countCreations := watchCreations(t, client, name)
	createBlob(t, client, name, sha, sourceWith(name, rejectHead, noRange), skipHead)

	blob := waitForBlobDone(t, client, name)
	created := countCreations()
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 1 {
		t.Errorf("blob %s was split into %d chunks, want 1", name, blob.Spec.ChunksNumber)
	}
	checkCreatedOnce(t, name, created)
}

func TestSkipHeadBlobWithOnlyChunkSizeIsSplitByIt(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-chunk-size")
	sha := writeSource(t, name, 16<<20)
	chunkSize := int64(6 << 20)
	countCreations := watchCreations(t, client, name)
	createBlob(t, client, name, sha, sourceWith(name, rejectHead), skipHead, chunkSizeOnly(chunkSize))

	blob := waitForBlobDone(t, client, name)
	created := countCreations()
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 3 || blob.Spec.ChunkSize != chunkSize {
		t.Errorf("blob %s was split into %d chunks of %d bytes, want 3 of %d", name, blob.Spec.ChunksNumber, blob.Spec.ChunkSize, chunkSize)
	}
	checkCreatedOnce(t, name, created)
}

func TestBlobWithOnlyChunkSizeIsSplitByIt(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-chunk-size")
	sha := writeSource(t, name, 16<<20)
	chunkSize := int64(6 << 20)
	countCreations := watchCreations(t, client, name)
	createBlob(t, client, name, sha, chunkSizeOnly(chunkSize))

	blob := waitForBlobDone(t, client, name)
	created := countCreations()
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 3 || blob.Spec.ChunkSize != chunkSize {
		t.Errorf("blob %s was split into %d chunks of %d bytes, want 3 of %d", name, blob.Spec.ChunksNumber, blob.Spec.ChunkSize, chunkSize)
	}
	checkCreatedOnce(t, name, created)
}

func TestSkipHeadBlobWithSkipIfExistsSucceedsWithoutTheBody(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-exists")
	size := int64(1 << 20)
	sha := writeSource(t, name, size)
	createBlob(t, client, name, sha)
	if blob := waitForBlobDone(t, client, name); blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}

	again := name + "-again"
	start := time.Now()
	createBlob(t, client, again, sha, sourceWith(name, rejectHead, slowBody), skipHead, func(spec *taskv1alpha1.BlobSpec) {
		spec.Destination[0].Path = "e2e/" + name
		spec.Destination[0].SkipIfExists = true
	})

	blob := waitForBlobDone(t, client, again)
	took := time.Since(start)
	// At the 1 KiB/s of slowBody.
	bodyTime := time.Duration(size>>10) * time.Second
	t.Logf("blob %s took %s; its body needs %s from %s", again, took.Round(time.Millisecond), bodyTime, slowBody)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", again, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if took > bodyTime/10 {
		t.Errorf("blob %s took %s, not far below the %s its body needs", again, took, bodyTime)
	}
}

func TestSkipHeadBlobSha256MismatchFails(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-mismatch")
	writeSource(t, name, 16<<20)
	createBlob(t, client, name, strings.Repeat("0", 64), sourceWith(name, rejectHead), skipHead, func(spec *taskv1alpha1.BlobSpec) {
		// Only the hash chain may report the mismatch; re-verification gives the same message.
		spec.Destination[0].ReverifySha256 = false
	})

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseFailed {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseFailed, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 3 {
		t.Errorf("blob %s was split into %d chunks, want 3", name, blob.Spec.ChunksNumber)
	}
	for _, condition := range blob.Status.Conditions {
		if strings.Contains(condition.Message, "sha256 mismatch") {
			return
		}
	}
	t.Errorf("blob %s failed without a sha256 mismatch condition; status: %s", name, statusJSON(blob))
}

func TestSkipHeadBlobFailsWhenTheSourceAnswersGetWith404(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	// Without writeSource nginx-test has nothing to serve under this name.
	name := uniqueName("e2e-skip-head-not-found")
	createBlob(t, client, name, "", sourceWith(name, rejectHead), skipHead)

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseFailed {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseFailed, statusJSON(blob))
	}
	// rejectHead answers every HEAD with 401, so only a GET gets the 404.
	for _, condition := range blob.Status.Conditions {
		if condition.Type == "UnexpectedStatusCode" && strings.Contains(condition.Message, "got 404") {
			return
		}
	}
	t.Errorf("blob %s failed without an UnexpectedStatusCode condition for 404; status: %s", name, statusJSON(blob))
}

func TestSkipHeadBlobOfEmptyContentSucceeds(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-empty")
	sha := writeSource(t, name, 0)
	countCreations := watchCreations(t, client, name)
	createBlob(t, client, name, sha, sourceWith(name, rejectHead), skipHead)

	// createBlob's ReverifySha256 hashes the destination object, so success means it is empty.
	blob := waitForBlobDone(t, client, name)
	created := countCreations()
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	checkCreatedOnce(t, name, created)
}

// sourceWith serves testdata/<name>.rand from nginx-test with the behaviours its query arguments choose.
func sourceWith(name string, args ...string) func(*taskv1alpha1.BlobSpec) {
	return func(spec *taskv1alpha1.BlobSpec) {
		spec.Source[0].URL = "http://cidn-nginx-test-1/" + name + ".rand?" + strings.Join(args, "&")
	}
}

func skipHead(spec *taskv1alpha1.BlobSpec) {
	spec.SkipHead = true
}

// chunkSizeOnly presets ChunkSize alone; createBlob's MinimumChunkSize would decide the layout otherwise.
func chunkSizeOnly(size int64) func(*taskv1alpha1.BlobSpec) {
	return func(spec *taskv1alpha1.BlobSpec) {
		spec.MinimumChunkSize = 0
		spec.ChunkSize = size
	}
}

// creations counts how often the Multipart record of a blob and each of its Chunks, by name, were created.
type creations struct {
	multipart int
	chunks    map[string]int
}

// watchCreations watches the Chunks and the Multipart record of the blob named blobName; the returned func ends the watch and counts their creations.
func watchCreations(t *testing.T, client versioned.Interface, blobName string) func() creations {
	t.Helper()
	chunks, err := client.TaskV1alpha1().Chunks().Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to watch chunks: %v", err)
	}
	t.Cleanup(chunks.Stop)
	multiparts, err := client.TaskV1alpha1().Multiparts().Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to watch multiparts: %v", err)
	}
	t.Cleanup(multiparts.Stop)

	created := creations{chunks: map[string]int{}}
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for {
			var event watch.Event
			var open bool
			select {
			case event, open = <-chunks.ResultChan():
			case event, open = <-multiparts.ResultChan():
			}
			if !open {
				return
			}
			if event.Type != watch.Added {
				continue
			}
			switch object := event.Object.(type) {
			case *taskv1alpha1.Chunk:
				if object.Annotations[blobNameAnnotation] == blobName {
					created.chunks[object.Name]++
				}
			case *taskv1alpha1.Multipart:
				// The controller names the record after its blob.
				if object.Name == blobName {
					created.multipart++
				}
			}
		}
	}()

	return func() creations {
		t.Helper()
		time.Sleep(chunkWatchSettle)
		select {
		case <-ended:
			t.Fatalf("the watch of blob %s ended early", blobName)
		default:
		}
		chunks.Stop()
		multiparts.Stop()
		<-ended
		return created
	}
}

// checkCreatedOnce checks that the blob's Multipart record was created once and none of its Chunks more than once.
func checkCreatedOnce(t *testing.T, blobName string, created creations) {
	t.Helper()
	if created.multipart != 1 {
		t.Errorf("multipart record of blob %s was created %d times, want once", blobName, created.multipart)
	}
	for _, name := range slices.Sorted(maps.Keys(created.chunks)) {
		if created.chunks[name] > 1 {
			t.Errorf("chunk %s was created %d times, want once", name, created.chunks[name])
		}
	}
}

// partChunkName is the controller's name for part num of a laid out blob.
func partChunkName(blob *taskv1alpha1.Blob, num int64) string {
	start := (num - 1) * blob.Spec.ChunkSize
	end := min(start+blob.Spec.ChunkSize, blob.Status.Total)
	numWidth := len(strconv.FormatInt(blob.Spec.ChunksNumber, 10))
	offsetWidth := len(strconv.FormatInt(blob.Status.Total, 16))
	return fmt.Sprintf("blob:part:%s:%0*d:%0*x-%0*x", blob.Name, numWidth, num, offsetWidth, start, offsetWidth, end)
}
