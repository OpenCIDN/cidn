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
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func setDuration(t *testing.T, variable *time.Duration, value time.Duration) {
	t.Helper()
	previous := *variable
	*variable = value
	t.Cleanup(func() { *variable = previous })
}

func expectFailedByUploads(t *testing.T, chunk *v1alpha1.Chunk, client *fake.Clientset, updates *callRecorder) {
	t.Helper()
	updates.mut.Lock()
	writes := slices.Clone(updates.objects)
	updates.mut.Unlock()
	for _, object := range writes {
		if status := object.(*v1alpha1.Chunk).Status; status.Phase == v1alpha1.ChunkPhasePending || status.HandlerName == "" {
			t.Fatalf("a status write reset the chunk: phase=%s handler=%q", status.Phase, status.HandlerName)
		}
	}
	status := waitForResult(t, client, chunk.Name).Status
	if _, found := v1alpha1.GetCondition(status.Conditions, "DestinationRequestError"); status.Phase != v1alpha1.ChunkPhaseFailed || !found ||
		!status.Retryable || status.HandlerName != chunk.Status.HandlerName || status.Retry != chunk.Status.Retry {
		t.Errorf("got phase=%s retryable=%t handler=%q retry=%d conditions=%v, want Failed, retryable with DestinationRequestError, handler %q and retry %d",
			status.Phase, status.Retryable, status.HandlerName, status.Retry, status.Conditions, chunk.Status.HandlerName, chunk.Status.Retry)
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
	updates := trackStatusUpdates(client, "chunks", nil)
	runProcess(runner, chunk)
	expectFailedByUploads(t, chunk, client, updates)
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
	updates := trackStatusUpdates(client, "chunks", nil)
	runProcess(runner, chunk)
	expectFailedByUploads(t, chunk, client, updates)
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

func expectFailedWith(t *testing.T, client *fake.Clientset, name, condition string, retryable bool) {
	t.Helper()
	status := waitForResult(t, client, name).Status
	if _, found := v1alpha1.GetCondition(status.Conditions, condition); status.Phase != v1alpha1.ChunkPhaseFailed || !found || status.Retryable != retryable {
		t.Errorf("got phase=%s retryable=%t conditions=%v, want Failed with %s and retryable=%t",
			status.Phase, status.Retryable, status.Conditions, condition, retryable)
	}
}

func TestSourceCutMidwayFailsTheChunkAsReadSourceErrorAtOnce(t *testing.T) {
	destination := startDestination(t, "etag-0", nil)
	chunk := newFirstPartChunk(startSource(t, testSource{content: testContent(64 << 10), cutAt: 16 << 10}), nil, destination)
	runner, client := newProcessRunner(t, chunk)
	started := time.Now()
	runProcess(runner, chunk)
	// Retrying the upload would first wait out the default interval of 5s.
	if took := time.Since(started); took >= time.Second {
		t.Errorf("process returned %v after it started, want within 1s", took)
	}
	expectFailedWith(t, client, chunk.Name, "ReadSourceError", true)
	if uploads := len(destination.received()); uploads > 1 {
		t.Errorf("the destination received %d uploads, want at most 1", uploads)
	}
}

func TestSourceCutMidwayAbortsAnUploadLeftUnanswered(t *testing.T) {
	// More than loopback buffers hold, so the upload is stuck writing when the source is cut.
	source := startSource(t, testSource{content: testContent(5 << 20), cutAt: 4 << 20})
	chunk := newFirstPartChunk(source, nil, startDestination(t, "etag-0", t.Context().Done()))
	runner, client := newProcessRunner(t, chunk)
	done := startProcess(t, runner, chunk)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not return within 5s although the source was cut")
	}
	expectFailedWith(t, client, chunk.Name, "ReadSourceError", true)
}

func TestUploadFailingForGoodHangsUpOnAStalledSource(t *testing.T) {
	hungUp := make(chan bool, 1)
	// net/http answers at once only when 256 KiB or more of the request body are left unread.
	source := startSource(t, testSource{content: testContent(1 << 20), pauseAfter: 64 << 10, hungUp: hungUp})
	chunk := newFirstPartChunk(source, nil, startFailingDestination(t, testFailure{status: http.StatusForbidden}))
	runner, client := newProcessRunner(t, chunk)
	done := startProcess(t, runner, chunk)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not return within 1s after the upload was rejected")
	}
	expectFailedWith(t, client, chunk.Name, "DestinationRequestError", false)
	select {
	case cut := <-hungUp:
		if !cut {
			t.Error("the source sent the rest of its body instead of seeing the runner hang up")
		}
	case <-time.After(5 * time.Second):
		t.Error("the source saw no hang-up within 5s after the upload was rejected")
	}
}

func TestUploadFailingForGoodAbortsTheUploadToAHungDestination(t *testing.T) {
	// Without a length the uploads start only after the whole source was read.
	for _, noLength := range []bool{false, true} {
		t.Run(fmt.Sprintf("noLength=%t", noLength), func(t *testing.T) {
			hung := startFailingDestination(t, testFailure{hangAt: 1})
			rejecting := startFailingDestination(t, testFailure{status: http.StatusForbidden})
			chunk := newFirstPartChunk(startSource(t, testSource{content: testContent(4096), noLength: noLength}), nil, hung, rejecting)
			runner, client := newProcessRunner(t, chunk)
			done := startProcess(t, runner, chunk)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("process did not return within 1s although one destination rejected its upload")
			}
			expectFailedWith(t, client, chunk.Name, "DestinationRequestError", false)
		})
	}
}
