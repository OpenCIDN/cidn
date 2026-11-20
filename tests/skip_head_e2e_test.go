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
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	taskv1alpha1 "github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// The controller ties every Chunk it creates to its Blob with this annotation.
	blobNameAnnotation = taskv1alpha1.GroupName + "/blob-name"
	// Time for the watch to deliver what was created just before the blob finished.
	chunkWatchSettle = 5 * time.Second
)

func TestBlobWithoutSkipHeadFailsWhenTheSourceRejectsHead(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-head-rejected")
	sha := writeSource(t, name, 1<<20)
	createBlob(t, client, name, sha, noHeadSource(name))

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
	chunkNames := watchChunkNames(t, client, name)
	createBlob(t, client, name, sha, noHeadSource(name), skipHead)

	blob := waitForBlobDone(t, client, name)
	names := chunkNames()
	t.Logf("chunks of blob %s: %v", name, names)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 3 {
		t.Errorf("blob %s was split into %d chunks, want 3", name, blob.Spec.ChunksNumber)
	}
	if !slices.Contains(names, "blob:first:"+name) {
		t.Errorf("blob %s had no first chunk; chunks: %v", name, names)
	}
	for _, chunk := range names {
		if strings.HasPrefix(chunk, "blob:head:"+name+":") || chunk == "blob:full:"+name {
			t.Errorf("blob %s had chunk %s; chunks: %v", name, chunk, names)
		}
	}
}

func TestSkipHeadBlobSingleChunkSucceeds(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-skip-head-single")
	sha := writeSource(t, name, 1<<20)
	createBlob(t, client, name, sha, noHeadSource(name), skipHead)

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 1 {
		t.Errorf("blob %s was split into %d chunks, want 1", name, blob.Spec.ChunksNumber)
	}
}

// noHeadSource serves testdata/<name>.rand from the nginx-test path that answers every HEAD with 401.
func noHeadSource(name string) func(*taskv1alpha1.BlobSpec) {
	return func(spec *taskv1alpha1.BlobSpec) {
		spec.Source[0].URL = "http://cidn-nginx-test-1/no-head/" + name + ".rand"
	}
}

func skipHead(spec *taskv1alpha1.BlobSpec) {
	spec.SkipHead = true
}

// watchChunkNames watches the Chunks of the blob named blobName; the returned func ends the watch and lists their names.
func watchChunkNames(t *testing.T, client versioned.Interface, blobName string) func() []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w, err := client.TaskV1alpha1().Chunks().Watch(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to watch chunks: %v", err)
	}

	names := map[string]bool{}
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for event := range w.ResultChan() {
			if chunk, ok := event.Object.(*taskv1alpha1.Chunk); ok && chunk.Annotations[blobNameAnnotation] == blobName {
				names[chunk.Name] = true
			}
		}
	}()

	return func() []string {
		t.Helper()
		time.Sleep(chunkWatchSettle)
		select {
		case <-ended:
			t.Fatalf("the chunk watch of blob %s ended early", blobName)
		default:
		}
		w.Stop()
		<-ended
		return slices.Sorted(maps.Keys(names))
	}
}
