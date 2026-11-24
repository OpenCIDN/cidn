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
	"slices"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestProcessReturnsWithoutWaitingAfterTheLastUploadAttempt(t *testing.T) {
	interval := 100 * time.Millisecond
	setDuration(t, &uploadRetryInterval, interval)
	destination := startFailingDestination(t, testFailure{})
	chunk := newFirstPartChunk(startSource(t, testSource{content: testContent(4096)}), nil, destination)
	runner, _ := newProcessRunner(t, chunk)
	runProcess(runner, chunk)
	returned := time.Now()
	arrivals := destination.arrived()
	if len(arrivals) != 5 {
		t.Fatalf("the destination received %d uploads, want 5", len(arrivals))
	}
	// The wait before the fifth attempt; one after it would be twice as long.
	if after := returned.Sub(arrivals[4]); after >= 8*interval {
		t.Errorf("process returned %v after the fifth upload arrived, want less than the %v waited before it", after, 8*interval)
	}
}

func startProcess(t *testing.T, runner *ChunkRunner, chunk *v1alpha1.Chunk) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProcess(runner, chunk)
	}()
	t.Cleanup(func() { <-done })
	return done
}

func waitUntil(t *testing.T, within time.Duration, what string, condition func() bool) time.Time {
	t.Helper()
	deadline := time.Now().Add(within)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, within)
		}
		time.Sleep(time.Millisecond)
	}
	return time.Now()
}

func TestStaleResetOfAHungUploadEndsTheChunkAtOnce(t *testing.T) {
	window := 500 * time.Millisecond
	// Over twice a window and an update period, so only the declared wait holds the reset off.
	interval := 1500 * time.Millisecond
	setDuration(t, &uploadRetryInterval, interval)
	setDuration(t, &staleResetAfter, window)
	destination := startFailingDestination(t, testFailure{hangAt: 2})
	chunk := newFirstPartChunk(startSource(t, testSource{content: testContent(4096)}), nil, destination)
	runner, client := newProcessRunner(t, chunk)
	runner.updateDuration = 10 * time.Millisecond
	updates := trackStatusUpdates(client, "chunks", nil)
	done := startProcess(t, runner, chunk)
	waitUntil(t, 2*interval, "the second upload", func() bool { return len(destination.arrived()) >= 2 })
	second := destination.arrived()[1]
	reset := waitUntil(t, 4*window, "a reset", func() bool {
		updates.mut.Lock()
		defer updates.mut.Unlock()
		return slices.ContainsFunc(updates.objects, func(object runtime.Object) bool {
			status := object.(*v1alpha1.Chunk).Status
			return status.Phase == v1alpha1.ChunkPhasePending && status.HandlerName == ""
		})
	})
	select {
	case <-done:
	case <-time.After(4 * interval):
		t.Fatalf("process did not return within %v after the reset", 4*interval)
	}
	if after := reset.Sub(second); after < window/2 {
		t.Errorf("the chunk was reset %v after its second upload began, want a stale window of %v", after, window)
	}
	if after := time.Since(reset); after >= interval {
		t.Errorf("process returned %v after the reset, want well within the next wait of %v", after, 2*interval)
	}
}

func TestDeletingTheChunkDuringARetryWaitEndsTheChunkAtOnce(t *testing.T) {
	interval := 2 * time.Second
	setDuration(t, &uploadRetryInterval, interval)
	destination := startFailingDestination(t, testFailure{})
	// A changing status makes the updater write, which is how it learns of the deletion.
	source := startSource(t, testSource{content: testContent(1 << 20), trickle: 5 * time.Millisecond})
	chunk := newFirstPartChunk(source, nil, destination)
	runner, client := newProcessRunner(t, chunk)
	runner.updateDuration = 10 * time.Millisecond
	done := startProcess(t, runner, chunk)
	waitUntil(t, time.Second, "the first upload", func() bool { return len(destination.arrived()) >= 1 })
	// Time for the first attempt to fail, well inside the wait after it.
	time.Sleep(100 * time.Millisecond)
	if err := client.TaskV1alpha1().Chunks().Delete(context.Background(), chunk.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deleted := time.Now()
	select {
	case <-done:
	case <-time.After(2 * interval):
		t.Fatalf("process did not return within %v after the chunk was deleted", 2*interval)
	}
	if after := time.Since(deleted); after >= interval/4 {
		t.Errorf("process returned %v after the chunk was deleted, want well within the %v wait", after, interval)
	}
	if uploads := len(destination.arrived()); uploads != 1 {
		t.Errorf("the destination received %d uploads, want 1", uploads)
	}
}
