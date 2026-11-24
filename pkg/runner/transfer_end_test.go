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
	"slices"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func setDuration(t *testing.T, variable *time.Duration, value time.Duration) {
	t.Helper()
	previous := *variable
	*variable = value
	t.Cleanup(func() { *variable = previous })
}

func recordUntilFailed(client *fake.Clientset) *callRecorder {
	// NotFound stops the updater, which could otherwise read staleResetAfter after the test restored it.
	client.PrependReactor("update", "chunks", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update := action.(k8stesting.UpdateAction)
		chunk := update.GetObject().(*v1alpha1.Chunk)
		if update.GetSubresource() != "status" || chunk.Status.Phase != v1alpha1.ChunkPhaseFailed {
			return false, nil, nil
		}
		return true, nil, apierrors.NewNotFound(v1alpha1.Resource("chunks"), chunk.Name)
	})
	return trackStatusUpdates(client, "chunks", nil)
}

func expectFailedByUploads(t *testing.T, chunk *v1alpha1.Chunk, updates *callRecorder) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		updates.mut.Lock()
		writes := slices.Clone(updates.objects)
		updates.mut.Unlock()
		for _, object := range writes {
			status := object.(*v1alpha1.Chunk).Status
			if status.Phase == v1alpha1.ChunkPhasePending || status.HandlerName == "" {
				t.Fatalf("a status write reset the chunk: phase=%s handler=%q", status.Phase, status.HandlerName)
			}
			if status.Phase != v1alpha1.ChunkPhaseFailed {
				continue
			}
			if _, found := v1alpha1.GetCondition(status.Conditions, "DestinationRequestError"); !found || !status.Retryable ||
				status.HandlerName != chunk.Status.HandlerName || status.Retry != chunk.Status.Retry {
				t.Errorf("got retryable=%t handler=%q retry=%d conditions=%v, want retryable with DestinationRequestError, handler %q and retry %d",
					status.Retryable, status.HandlerName, status.Retry, status.Conditions, chunk.Status.HandlerName, chunk.Status.Retry)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no Failed status was written within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUploadsFailingEveryTimeFailTheChunkInsteadOfResettingIt(t *testing.T) {
	setDuration(t, &uploadRetryInterval, 100*time.Millisecond)
	// Shorter than the last two waits, which are 400 and 800 ms.
	setDuration(t, &staleResetAfter, 200*time.Millisecond)
	destination := startFailingDestination(t, testFailure{})
	chunk := newFirstPartChunk(startSource(t, testSource{content: testContent(4096)}), nil, destination)
	runner, client := newProcessRunner(t, chunk)
	runner.updateDuration = 10 * time.Millisecond
	updates := recordUntilFailed(client)
	runProcess(runner, chunk)
	expectFailedByUploads(t, chunk, updates)
	if uploads := len(destination.arrived()); uploads != 5 {
		t.Errorf("the destination received %d uploads, want 5", uploads)
	}
}

func TestUploadsFailingSlowlyFailTheChunkInsteadOfResettingIt(t *testing.T) {
	window := 200 * time.Millisecond
	setDuration(t, &uploadRetryInterval, 10*time.Millisecond)
	setDuration(t, &staleResetAfter, window)
	// Each attempt takes half a window, all five together more than two.
	destination := startFailingDestination(t, testFailure{cutAfter: window / 2})
	chunk := newFirstPartChunk(startSource(t, testSource{content: testContent(4096)}), nil, destination)
	runner, client := newProcessRunner(t, chunk)
	runner.updateDuration = 10 * time.Millisecond
	updates := recordUntilFailed(client)
	runProcess(runner, chunk)
	expectFailedByUploads(t, chunk, updates)
	if uploads := len(destination.arrived()); uploads != 5 {
		t.Errorf("the destination received %d uploads, want 5", uploads)
	}
}
