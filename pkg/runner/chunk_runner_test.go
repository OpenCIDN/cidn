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
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/wzshiming/ioswmr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func newTestRunner(t *testing.T, concurrency int, chunks []*v1alpha1.Chunk, bearers []*v1alpha1.Bearer) (*ChunkRunner, *fake.Clientset) {
	t.Helper()
	objects := make([]runtime.Object, 0, len(chunks)+len(bearers))
	for _, chunk := range chunks {
		objects = append(objects, chunk)
	}
	for _, bearer := range bearers {
		objects = append(objects, bearer)
	}
	client := fake.NewSimpleClientset(objects...)
	factory := externalversions.NewSharedInformerFactory(client, 0)
	runner := NewChunkRunner("runner-test", client, factory, time.Second, concurrency)
	for _, chunk := range chunks {
		if err := runner.chunkInformer.Informer().GetIndexer().Add(chunk); err != nil {
			t.Fatal(err)
		}
	}
	for _, bearer := range bearers {
		if err := runner.bearerInformer.Informer().GetIndexer().Add(bearer); err != nil {
			t.Fatal(err)
		}
	}
	return runner, client
}

func newTestChunk(name string, priority, retry int64) *v1alpha1.Chunk {
	return &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.ChunkSpec{Priority: priority},
		Status:     v1alpha1.ChunkStatus{Phase: v1alpha1.ChunkPhasePending, Retry: retry},
	}
}

func TestReleaseReleasesOwnedActiveChunksOutsideInformer(t *testing.T) {
	var chunks []*v1alpha1.Chunk
	for _, phase := range []v1alpha1.ChunkPhase{v1alpha1.ChunkPhaseRunning, v1alpha1.ChunkPhasePending, v1alpha1.ChunkPhaseUnknown, v1alpha1.ChunkPhaseSucceeded} {
		chunk := newTestChunk(string(phase), 0, 0)
		chunk.Status.HandlerName = "runner-test"
		chunk.Status.Phase = phase
		chunk.Status.Conditions = []v1alpha1.Condition{{Type: "Existing", Message: "keep unless released"}}
		chunks = append(chunks, chunk)
	}
	other := newTestChunk("other", 0, 0)
	other.Status.HandlerName = "other"
	other.Status.Phase = v1alpha1.ChunkPhaseRunning
	chunks = append(chunks, other)
	runner, client := newTestRunner(t, 1, chunks, nil)
	if err := runner.chunkInformer.Informer().GetIndexer().Replace(nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := runner.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		got, err := client.TaskV1alpha1().Chunks().Get(context.Background(), chunk.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := chunk.DeepCopy()
		if chunk.Status.HandlerName == runner.handlerName && chunk.Status.Phase != v1alpha1.ChunkPhaseSucceeded {
			want.Status.HandlerName = ""
			want.Status.Phase = v1alpha1.ChunkPhasePending
			want.Status.Conditions = nil
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("chunk %s: got status %+v, want %+v", chunk.Name, got.Status, want.Status)
		}
	}
}

func TestReleaseKeepsChunkThatFinishedDuringRelease(t *testing.T) {
	chunk := newTestChunk("finishing", 0, 0)
	chunk.Status.HandlerName = "runner-test"
	chunk.Status.Phase = v1alpha1.ChunkPhaseRunning
	runner, client := newTestRunner(t, 1, []*v1alpha1.Chunk{chunk}, nil)
	succeeded := chunk.DeepCopy()
	succeeded.Status.Phase = v1alpha1.ChunkPhaseSucceeded
	succeeded.Status.Etags = []string{"e1"}
	gvr := v1alpha1.SchemeGroupVersion.WithResource("chunks")
	conflicted := false
	client.PrependReactor("update", "chunks", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if conflicted || action.(k8stesting.UpdateAction).GetSubresource() != "status" {
			return false, nil, nil
		}
		conflicted = true
		if err := client.Tracker().Update(gvr, succeeded, ""); err != nil {
			t.Error(err)
		}
		return true, nil, apierrors.NewConflict(v1alpha1.Resource("chunks"), chunk.Name, errors.New("finished meanwhile"))
	})
	if err := runner.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := client.TaskV1alpha1().Chunks().Get(context.Background(), chunk.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Status, succeeded.Status) {
		t.Errorf("release reverted a finished chunk: got %+v, want %+v", got.Status, succeeded.Status)
	}
}

func TestProgressUpdaterPreservesWaitingChunksButResetsStalledDownloads(t *testing.T) {
	previous := staleResetAfter
	staleResetAfter = 50 * time.Millisecond
	var states []*state
	t.Cleanup(func() {
		for _, current := range states {
			current.mut.Lock()
			defer current.mut.Unlock()
		}
		staleResetAfter = previous
	})
	for _, waiting := range []bool{true, false} {
		t.Run(fmt.Sprintf("waiting-%t", waiting), func(t *testing.T) {
			chunk := newTestChunk("running", 0, 0)
			chunk.Status.HandlerName = "runner-test"
			chunk.Status.Phase = v1alpha1.ChunkPhaseRunning
			runner, client := newTestRunner(t, 1, []*v1alpha1.Chunk{chunk}, nil)
			runner.updateDuration = 10 * time.Millisecond
			updates := trackStatusUpdates(client, "chunks", nil)
			current := newState(chunk)
			current.waiting.Store(waiting)
			states = append(states, current)
			ctx, cancel := context.WithCancel(context.Background())
			var source *readCount
			var destinations []*swmrCount
			stop := runner.startProgressUpdater(ctx, cancel, current, &source, &destinations)
			t.Cleanup(func() {
				stop()
				cancel()
			})
			if waiting {
				time.Sleep(300 * time.Millisecond)
				if ctx.Err() != nil {
					t.Error("waiting chunk was cancelled by stale reset")
				}
			} else {
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
					t.Error("stalled download was not cancelled within 1s")
				}
			}
			updates.mut.Lock()
			defer updates.mut.Unlock()
			reset := false
			for _, object := range updates.objects {
				status := object.(*v1alpha1.Chunk).Status
				if status.Phase == v1alpha1.ChunkPhasePending || status.HandlerName == "" {
					reset = true
					if status.Phase != v1alpha1.ChunkPhasePending || status.HandlerName != "" {
						t.Errorf("incomplete reset: phase=%s handler=%q", status.Phase, status.HandlerName)
					}
				}
			}
			if reset == waiting {
				t.Errorf("waiting=%t: reset update observed=%t", waiting, reset)
			}
		})
	}
}

func TestProgressUpdaterKeepsLocalResultAfterConflict(t *testing.T) {
	chunk := newTestChunk("finished", 0, 0)
	chunk.Status.HandlerName = "runner-test"
	chunk.Status.Phase = v1alpha1.ChunkPhaseUnknown
	runner, client := newTestRunner(t, 1, []*v1alpha1.Chunk{chunk}, nil)
	runner.updateDuration = 10 * time.Millisecond
	updates := trackStatusUpdates(client, "chunks", nil)
	first := true
	client.PrependReactor("update", "chunks", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" && first {
			first = false
			return true, nil, apierrors.NewConflict(v1alpha1.Resource("chunks"), chunk.Name, errors.New("controller changed status"))
		}
		return false, nil, nil
	})
	local := chunk.DeepCopy()
	local.Status.Phase = v1alpha1.ChunkPhaseSucceeded
	local.Status.Etags = []string{"e1"}
	current := newState(local)
	ctx, cancel := context.WithCancel(context.Background())
	var source *readCount
	var destinations []*swmrCount
	stop := sync.OnceFunc(runner.startProgressUpdater(ctx, cancel, current, &source, &destinations))
	t.Cleanup(func() {
		stop()
		cancel()
	})
	stop()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := client.TaskV1alpha1().Chunks().Get(context.Background(), chunk.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase == v1alpha1.ChunkPhaseSucceeded && reflect.DeepEqual(got.Status.Etags, []string{"e1"}) {
			if len(updates.snapshot()) == 0 {
				t.Fatal("no status retry recorded")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("conflict lost local result: phase=%s etags=%v", got.Status.Phase, got.Status.Etags)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWaitForPartialChunkStopsWhenOwnershipIsLost(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing-%t", missing), func(t *testing.T) {
			previous := newTestChunk("A", 0, 0)
			previous.Status.Phase = v1alpha1.ChunkPhaseRunning
			chunk := newTestChunk("B", 0, 0)
			chunk.Status.HandlerName = "other"
			chunk.Status.Phase = v1alpha1.ChunkPhaseRunning
			chunk.Spec.Sha256PartialPreviousName = previous.Name
			chunks := []*v1alpha1.Chunk{previous}
			if !missing {
				chunks = append(chunks, chunk)
			}
			runner, client := newTestRunner(t, 1, chunks, nil)
			runner.updateDuration = 10 * time.Millisecond
			local := chunk.DeepCopy()
			local.Status.HandlerName = runner.handlerName
			current := newState(local)
			file, err := os.CreateTemp(t.TempDir(), "chunk-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := file.Close(); err != nil {
					t.Error(err)
				}
			})
			swmr := ioswmr.NewSWMR(file)
			done := make(chan struct{})
			t.Cleanup(func() {
				if err := client.TaskV1alpha1().Chunks().Delete(context.Background(), previous.Name, metav1.DeleteOptions{}); err != nil {
					t.Error(err)
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("waiter did not exit during cleanup")
				}
			})
			go func() {
				runner.waitForPartialChunk(current, swmr, nil)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("waiter did not stop within 3s after ownership was lost")
			}
			if phase := current.Get().Status.Phase; phase != v1alpha1.ChunkPhaseRunning {
				t.Errorf("waiter wrote a result after ownership was lost: phase=%s", phase)
			}
		})
	}
}

func TestGetPendingListTieredRandomOrder(t *testing.T) {
	tiers := []struct {
		priority int64
		retry    int64
	}{{10, 0}, {10, 1}, {5, 0}}
	var chunks []*v1alpha1.Chunk
	expectedNames := make(map[string]bool)
	for tierIndex, tier := range tiers {
		for index := 0; index < 10; index++ {
			name := fmt.Sprintf("tier-%d-chunk-%02d", tierIndex, index)
			chunks = append(chunks, newTestChunk(name, tier.priority, tier.retry))
			expectedNames[name] = true
		}
	}
	assigned := newTestChunk("assigned", 10, 0)
	assigned.Status.HandlerName = "other-runner"
	running := newTestChunk("running", 10, 0)
	running.Status.Phase = v1alpha1.ChunkPhaseRunning
	chunks = append(chunks, assigned, running)
	runner, _ := newTestRunner(t, 1, chunks, nil)
	sequences := make(map[string]bool)
	for attempt := 0; attempt < 20; attempt++ {
		pending, err := runner.getPendingList()
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 30 {
			t.Fatalf("attempt %d: got %d chunks, want 30", attempt, len(pending))
		}
		seen := make(map[string]bool)
		var firstTier []string
		for index, chunk := range pending {
			if !expectedNames[chunk.Name] || seen[chunk.Name] {
				t.Fatalf("attempt %d: unexpected or duplicate chunk %q", attempt, chunk.Name)
			}
			seen[chunk.Name] = true
			tier := tiers[index/10]
			if chunk.Spec.Priority != tier.priority || chunk.Status.Retry != tier.retry {
				t.Fatalf("attempt %d, position %d: got tier (%d,%d), want (%d,%d)",
					attempt, index, chunk.Spec.Priority, chunk.Status.Retry, tier.priority, tier.retry)
			}
			if index < 10 {
				firstTier = append(firstTier, chunk.Name)
			}
		}
		sequences[strings.Join(firstTier, ",")] = true
	}
	if len(sequences) < 2 {
		t.Fatal("first tier order did not vary across 20 calls")
	}
}

type callRecorder struct {
	mut     sync.Mutex
	names   []string
	objects []runtime.Object
}

func trackStatusUpdates(client *fake.Clientset, resource string, conflicts map[string]bool) *callRecorder {
	recorder := &callRecorder{}
	client.PrependReactor("update", resource, func(action k8stesting.Action) (bool, runtime.Object, error) {
		update := action.(k8stesting.UpdateAction)
		if update.GetSubresource() != "status" {
			return false, nil, nil
		}
		name := update.GetObject().(metav1.Object).GetName()
		recorder.mut.Lock()
		recorder.names = append(recorder.names, name)
		recorder.objects = append(recorder.objects, update.GetObject().DeepCopyObject())
		recorder.mut.Unlock()
		if conflicts[name] {
			return true, nil, apierrors.NewConflict(v1alpha1.Resource(resource), name, errors.New("already claimed"))
		}
		return false, nil, nil
	})
	return recorder
}

func (recorder *callRecorder) snapshot() []string {
	recorder.mut.Lock()
	defer recorder.mut.Unlock()
	return append([]string(nil), recorder.names...)
}

func heldCallbacks(t *testing.T) (func(*v1alpha1.Chunk), <-chan string) {
	t.Helper()
	names := make(chan string, 20)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	return func(chunk *v1alpha1.Chunk) {
		names <- chunk.Name
		<-release
	}, names
}

func expectCallbacks(t *testing.T, names <-chan string, count int) {
	t.Helper()
	seen := make(map[string]bool)
	for index := 0; index < count; index++ {
		select {
		case name := <-names:
			if seen[name] {
				t.Errorf("duplicate callback for %s", name)
			}
			seen[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("got %d callbacks, want %d", index, count)
		}
	}
	select {
	case name := <-names:
		t.Errorf("unexpected extra callback for %s", name)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHandlePendingUnmarksConflicts(t *testing.T) {
	for _, concurrency := range []int{10, 4} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			var chunks []*v1alpha1.Chunk
			for index := 0; index < 10; index++ {
				chunks = append(chunks, newTestChunk(fmt.Sprintf("chunk-%d", index), 0, 0))
			}
			runner, client := newTestRunner(t, concurrency, chunks, nil)
			conflicts := map[string]bool{"chunk-0": true, "chunk-1": true, "chunk-2": true}
			updates := trackStatusUpdates(client, "chunks", conflicts)
			callback, names := heldCallbacks(t)
			stats, err := runner.handlePending(context.Background(), chunks, callback)
			if err != nil {
				t.Fatal(err)
			}
			want := min(concurrency, 7)
			if stats.conflicts != 3 || stats.acquired != want {
				t.Errorf("got stats %+v, want conflicts=3 acquired=%d", stats, want)
			}
			if got := len(updates.snapshot()); got != want+3 {
				t.Errorf("got %d chunk updates, want %d", got, want+3)
			}
			for name := range conflicts {
				if !runner.markRecord(name) {
					t.Errorf("conflicting chunk %s stayed marked", name)
				}
			}
			expectCallbacks(t, names, want)
		})
	}
}

func TestHandlePendingRejectsStaleSnapshot(t *testing.T) {
	chunks := []*v1alpha1.Chunk{
		newTestChunk("assigned", 0, 0),
		newTestChunk("deleted", 0, 0),
		newTestChunk("pending", 0, 0),
	}
	runner, client := newTestRunner(t, 3, chunks, nil)
	snapshot, err := runner.getPendingList()
	if err != nil {
		t.Fatal(err)
	}
	assigned := chunks[0].DeepCopy()
	assigned.Status.HandlerName = "other"
	indexer := runner.chunkInformer.Informer().GetIndexer()
	if err := indexer.Update(assigned); err != nil {
		t.Fatal(err)
	}
	if err := indexer.Delete(chunks[1]); err != nil {
		t.Fatal(err)
	}
	latest := chunks[2].DeepCopy()
	latest.ResourceVersion = "new-version"
	latest.Status.Retry = 2
	if err := indexer.Update(latest); err != nil {
		t.Fatal(err)
	}
	updates := trackStatusUpdates(client, "chunks", nil)
	callback, names := heldCallbacks(t)
	stats, err := runner.handlePending(context.Background(), snapshot, callback)
	if err != nil {
		t.Fatal(err)
	}
	if stats.stale != 2 || stats.acquired != 1 {
		t.Errorf("got stats %+v, want stale=2 acquired=1", stats)
	}
	for _, name := range updates.snapshot() {
		if name != "pending" {
			t.Errorf("stale chunk %s received a status update", name)
		}
	}
	updated, err := client.TaskV1alpha1().Chunks().Get(context.Background(), latest.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.ResourceVersion != latest.ResourceVersion || updated.Status.Retry != latest.Status.Retry {
		t.Errorf("claim used old snapshot: version=%s retry=%d", updated.ResourceVersion, updated.Status.Retry)
	}
	if latest.Status.HandlerName != "" || latest.Status.Phase != v1alpha1.ChunkPhasePending {
		t.Error("claim mutated the informer cache object")
	}
	expectCallbacks(t, names, 1)
}

func TestHandlePendingDeduplicatesExpiredBearerReset(t *testing.T) {
	for _, phase := range []v1alpha1.BearerPhase{v1alpha1.BearerPhaseSucceeded, v1alpha1.BearerPhaseRunning} {
		t.Run(string(phase), func(t *testing.T) {
			bearer := &v1alpha1.Bearer{
				ObjectMeta: metav1.ObjectMeta{Name: "expired"},
				Status: v1alpha1.BearerStatus{
					Phase: phase,
					TokenInfo: &v1alpha1.BearerTokenInfo{
						IssuedAt:  metav1.NewTime(time.Now().Add(-2 * time.Hour)),
						ExpiresIn: 3600,
					},
				},
			}
			var chunks []*v1alpha1.Chunk
			for index := 0; index < 3; index++ {
				chunk := newTestChunk(fmt.Sprintf("chunk-%d", index), 0, 0)
				chunk.Spec.BearerName = bearer.Name
				chunks = append(chunks, chunk)
			}
			runner, client := newTestRunner(t, 3, chunks, []*v1alpha1.Bearer{bearer})
			chunkUpdates := trackStatusUpdates(client, "chunks", nil)
			bearerUpdates := trackStatusUpdates(client, "bearers", nil)
			callback, names := heldCallbacks(t)
			stats, err := runner.handlePending(context.Background(), chunks, callback)
			if !errors.Is(err, ErrNoPendingChunk) {
				t.Errorf("got error %v, want ErrNoPendingChunk", err)
			}
			if stats.skipped != 3 || stats.acquired != 0 {
				t.Errorf("got stats %+v, want skipped=3 acquired=0", stats)
			}
			want := 0
			if phase == v1alpha1.BearerPhaseSucceeded {
				want = 1
			}
			if got := len(bearerUpdates.snapshot()); got != want {
				t.Errorf("expired %s bearer reset %d times, want %d", phase, got, want)
			}
			if got := len(chunkUpdates.snapshot()); got != 0 {
				t.Errorf("got %d chunk updates with expired bearer, want 0", got)
			}
			expectCallbacks(t, names, 0)
		})
	}
}

func TestHandlePendingFillsOnlyFreeSlots(t *testing.T) {
	var chunks []*v1alpha1.Chunk
	for index := 0; index < 10; index++ {
		chunks = append(chunks, newTestChunk(fmt.Sprintf("chunk-%d", index), 0, 0))
	}
	runner, client := newTestRunner(t, 4, chunks, nil)
	updates := trackStatusUpdates(client, "chunks", nil)
	callback, names := heldCallbacks(t)
	stats, err := runner.handlePending(context.Background(), chunks, callback)
	if err != nil {
		t.Fatal(err)
	}
	if stats.acquired != 4 || stats.free != 4 || stats.pending != 10 {
		t.Errorf("got stats %+v, want acquired=4 free=4 pending=10", stats)
	}
	if got := len(runner.concurrencySem); got != 4 {
		t.Errorf("got %d held slots, want 4", got)
	}
	if got := len(updates.snapshot()); got != 4 {
		t.Errorf("got %d chunk updates, want 4", got)
	}
	expectCallbacks(t, names, 4)
}

func TestHandlePendingSkipsBearerWithoutToken(t *testing.T) {
	chunk := newTestChunk("pending", 0, 0)
	chunk.Spec.BearerName = "not-ready"
	bearer := &v1alpha1.Bearer{ObjectMeta: metav1.ObjectMeta{Name: chunk.Spec.BearerName}}
	runner, client := newTestRunner(t, 1, []*v1alpha1.Chunk{chunk}, []*v1alpha1.Bearer{bearer})
	updates := trackStatusUpdates(client, "chunks", nil)
	callback, names := heldCallbacks(t)
	stats, err := runner.handlePending(context.Background(), []*v1alpha1.Chunk{chunk}, callback)
	if !errors.Is(err, ErrNoPendingChunk) {
		t.Errorf("got error %v, want ErrNoPendingChunk", err)
	}
	if stats.skipped != 1 || stats.acquired != 0 {
		t.Errorf("got stats %+v, want skipped=1 acquired=0", stats)
	}
	if got := len(updates.snapshot()); got != 0 {
		t.Errorf("got %d chunk updates without bearer token, want 0", got)
	}
	expectCallbacks(t, names, 0)
}

func TestHandlePendingSkipsOnlyPendingPredecessor(t *testing.T) {
	for _, phase := range []v1alpha1.ChunkPhase{v1alpha1.ChunkPhasePending, "", v1alpha1.ChunkPhaseRunning} {
		t.Run(string(phase), func(t *testing.T) {
			chunk := newTestChunk("B", 0, 0)
			chunk.Spec.Sha256PartialPreviousName = "A"
			chunks := []*v1alpha1.Chunk{chunk}
			if phase != "" {
				previous := newTestChunk("A", 0, 0)
				previous.Status.Phase = phase
				chunks = append(chunks, previous)
			}
			runner, client := newTestRunner(t, 1, chunks, nil)
			updates := trackStatusUpdates(client, "chunks", nil)
			callback, names := heldCallbacks(t)
			stats, err := runner.handlePending(context.Background(), []*v1alpha1.Chunk{chunk}, callback)
			want := 1
			if phase == v1alpha1.ChunkPhasePending {
				want = 0
				if !errors.Is(err, ErrNoPendingChunk) {
					t.Errorf("got error %v, want ErrNoPendingChunk", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if stats.acquired != want || stats.skipped != 1-want {
				t.Errorf("got stats %+v, want acquired=%d skipped=%d", stats, want, 1-want)
			}
			if got := len(updates.snapshot()); got != want {
				t.Errorf("got %d chunk updates with predecessor phase %q, want %d", got, phase, want)
			}
			expectCallbacks(t, names, want)
		})
	}
}

func TestHandlePendingSlotReleaseSignalsWorker(t *testing.T) {
	chunk := newTestChunk("pending", 0, 0)
	runner, _ := newTestRunner(t, 1, []*v1alpha1.Chunk{chunk}, nil)
	names := make(chan string, 2)
	stats, err := runner.handlePending(context.Background(), []*v1alpha1.Chunk{chunk}, func(chunk *v1alpha1.Chunk) {
		names <- chunk.Name
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.acquired != 1 {
		t.Errorf("got stats %+v, want acquired=1", stats)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(runner.concurrencySem) != 0 || len(runner.signal) != 1 {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("slot release did not wake worker: slots=%d signals=%d", len(runner.concurrencySem), len(runner.signal))
		}
	}
	expectCallbacks(t, names, 1)
}
