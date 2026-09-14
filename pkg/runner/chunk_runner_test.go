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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
