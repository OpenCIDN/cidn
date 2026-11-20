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

package blob

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

const (
	mib = 1024 * 1024
	gib = 1024 * mib
)

var testStrategy, testStatusStrategy = newTestStrategies()

func newTestStrategies() (*blobStrategy, *blobStatusStrategy) {
	scheme := runtime.NewScheme()
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	strategy := NewStrategy(scheme)
	return strategy, NewStatusStrategy(strategy)
}

func testContext() context.Context {
	return genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceNone)
}

func newBlob(spec v1alpha1.BlobSpec) *v1alpha1.Blob {
	return &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{Name: "blob", ResourceVersion: "1"},
		Spec:       spec,
		Status:     v1alpha1.BlobStatus{Phase: v1alpha1.BlobPhaseRunning},
	}
}

func update(strategy rest.RESTUpdateStrategy, old *v1alpha1.Blob, change func(*v1alpha1.Blob)) (*v1alpha1.Blob, error) {
	obj := old.DeepCopy()
	change(obj)
	err := rest.BeforeUpdate(strategy, testContext(), obj, old)
	return obj, err
}

func mustUpdate(t *testing.T, strategy rest.RESTUpdateStrategy, old *v1alpha1.Blob, change func(*v1alpha1.Blob)) *v1alpha1.Blob {
	t.Helper()
	obj, err := update(strategy, old, change)
	if err != nil {
		t.Fatalf("update rejected: %v", err)
	}
	return obj
}

func setTotal(total int64, acceptRanges bool) func(*v1alpha1.Blob) {
	return func(blob *v1alpha1.Blob) {
		blob.Status.Total, blob.Status.AcceptRanges = total, acceptRanges
	}
}

func addProgress(blob *v1alpha1.Blob) {
	blob.Status.Progress += mib
}

func checkLayout(t *testing.T, when string, blob *v1alpha1.Blob, chunksNumber, chunkSize int64) {
	t.Helper()
	if blob.Spec.ChunksNumber != chunksNumber || blob.Spec.ChunkSize != chunkSize {
		t.Errorf("%s: %d chunks of %d, want %d chunks of %d", when, blob.Spec.ChunksNumber, blob.Spec.ChunkSize, chunksNumber, chunkSize)
	}
}

func TestChunkSizeOnlyLayoutSurvivesStatusUpdates(t *testing.T) {
	blob := mustUpdate(t, testStatusStrategy, newBlob(v1alpha1.BlobSpec{ChunkSize: 16 * mib}), setTotal(50*mib, true))
	checkLayout(t, "when the total is set", blob, 4, 16*mib)
	for i := 1; i <= 2; i++ {
		blob = mustUpdate(t, testStatusStrategy, blob, addProgress)
		checkLayout(t, fmt.Sprintf("after status update %d", i), blob, 4, 16*mib)
	}
}

func TestChunksNumberOnlyLayoutSurvivesStatusUpdates(t *testing.T) {
	blob := mustUpdate(t, testStatusStrategy, newBlob(v1alpha1.BlobSpec{ChunksNumber: 4}), setTotal(64*mib, true))
	checkLayout(t, "when the total is set", blob, 4, 16*mib)
	blob = mustUpdate(t, testStatusStrategy, blob, addProgress)
	checkLayout(t, "after a later status update", blob, 4, 16*mib)
}

func TestLayoutSetAtCreationSurvivesStatusUpdates(t *testing.T) {
	blob := &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{Name: "blob"},
		Spec: v1alpha1.BlobSpec{
			Source:      []v1alpha1.BlobSource{{URL: "http://example.com/blob"}},
			Destination: []v1alpha1.BlobDestination{{Name: "storage", Path: "blob"}},
			ChunkSize:   16 * mib,
		},
		Status: v1alpha1.BlobStatus{Total: 50 * mib, AcceptRanges: true},
	}
	rest.FillObjectMetaSystemFields(blob)
	if err := rest.BeforeCreate(testStrategy, testContext(), blob); err != nil {
		t.Fatalf("create rejected: %v", err)
	}
	checkLayout(t, "at creation", blob, 4, 16*mib)
	blob.ResourceVersion = "1" // assigned by the storage
	blob = mustUpdate(t, testStatusStrategy, blob, addProgress)
	checkLayout(t, "after a status update", blob, 4, 16*mib)
}

func TestSpecUpdateKeepsTheLayout(t *testing.T) {
	blob := mustUpdate(t, testStatusStrategy, newBlob(v1alpha1.BlobSpec{ChunkSize: 16 * mib}), setTotal(50*mib, true))
	blob = mustUpdate(t, testStrategy, blob, func(blob *v1alpha1.Blob) { blob.Spec.Priority = 1 })
	checkLayout(t, "after a spec update", blob, 4, 16*mib)
}

func TestOtherPresetsKeepTheirLayoutAfterStatusUpdates(t *testing.T) {
	tests := []struct {
		name             string
		spec             v1alpha1.BlobSpec
		total            int64
		acceptRanges     bool
		wantChunksNumber int64
		wantChunkSize    int64
	}{
		{name: "minimum chunk size only", spec: v1alpha1.BlobSpec{MinimumChunkSize: 5 * mib}, total: 16 * mib, acceptRanges: true, wantChunksNumber: 3, wantChunkSize: 5592406},
		{name: "chunk size and chunks number", spec: v1alpha1.BlobSpec{ChunkSize: 16 * mib, ChunksNumber: 4}, total: 64 * mib, acceptRanges: true, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "no presets", total: 64 * mib, acceptRanges: true, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "not chunkable", spec: v1alpha1.BlobSpec{MinimumChunkSize: 5 * mib}, total: 16 * mib, wantChunksNumber: 1, wantChunkSize: 16 * mib},
		{name: "chunks number one", spec: v1alpha1.BlobSpec{ChunksNumber: 1}, total: 64 * mib, wantChunksNumber: 1, wantChunkSize: 64 * mib},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := mustUpdate(t, testStatusStrategy, newBlob(tt.spec), setTotal(tt.total, tt.acceptRanges))
			checkLayout(t, "when the total is set", blob, tt.wantChunksNumber, tt.wantChunkSize)
			blob = mustUpdate(t, testStatusStrategy, blob, addProgress)
			checkLayout(t, "after a later status update", blob, tt.wantChunksNumber, tt.wantChunkSize)
		})
	}
}

func TestStatusUpdateSettingTheTotalIsAccepted(t *testing.T) {
	presets := []v1alpha1.BlobSpec{
		{},
		{MinimumChunkSize: 5 * mib},
		{ChunkSize: 6 * mib},
		{ChunksNumber: 2},
		{ChunkSize: 6 * mib, ChunksNumber: 2},
		{MinimumChunkSize: 5 * mib, ChunkSize: 6 * mib},
		{MinimumChunkSize: 5 * mib, ChunksNumber: 2},
		{MinimumChunkSize: 5 * mib, ChunkSize: 6 * mib, ChunksNumber: 2},
	}
	totals := []struct {
		total        int64
		acceptRanges bool
	}{
		{total: 5 * mib, acceptRanges: true},
		{total: 16 * mib, acceptRanges: true},
		{total: 20 * gib, acceptRanges: true},
		{total: 16 * mib},
		{total: -1},
	}
	for _, spec := range presets {
		for _, tt := range totals {
			name := fmt.Sprintf("minimum %d chunk size %d chunks %d total %d accept ranges %t", spec.MinimumChunkSize, spec.ChunkSize, spec.ChunksNumber, tt.total, tt.acceptRanges)
			t.Run(name, func(t *testing.T) {
				if _, err := update(testStatusStrategy, newBlob(spec), setTotal(tt.total, tt.acceptRanges)); err != nil {
					t.Errorf("update rejected: %v", err)
				}
			})
		}
	}
}

func TestUnknownTotalStaysOneChunkWhenTheSizeIsBackfilled(t *testing.T) {
	blob := mustUpdate(t, testStatusStrategy, newBlob(v1alpha1.BlobSpec{MinimumChunkSize: 5 * mib}), setTotal(-1, false))
	if blob.Spec.ChunksNumber != 1 {
		t.Fatalf("unknown total: %d chunks, want 1", blob.Spec.ChunksNumber)
	}
	blob = mustUpdate(t, testStatusStrategy, blob, setTotal(16*mib, false))
	if blob.Spec.ChunksNumber != 1 {
		t.Errorf("backfilled total: %d chunks, want 1", blob.Spec.ChunksNumber)
	}
}

func TestSpecUpdateChangingAChunkPresetIsRejected(t *testing.T) {
	determined := mustUpdate(t, testStatusStrategy, newBlob(v1alpha1.BlobSpec{MinimumChunkSize: 5 * mib}), setTotal(16*mib, true))
	tests := []struct {
		name   string
		old    *v1alpha1.Blob
		change func(*v1alpha1.Blob)
		want   string
	}{
		{name: "preset chunk size", old: newBlob(v1alpha1.BlobSpec{ChunkSize: 16 * mib}), change: func(blob *v1alpha1.Blob) { blob.Spec.ChunkSize = 8 * mib }, want: "chunkSize is immutable"},
		{name: "preset chunks number", old: newBlob(v1alpha1.BlobSpec{ChunksNumber: 4}), change: func(blob *v1alpha1.Blob) { blob.Spec.ChunksNumber = 8 }, want: "chunksNumber is immutable"},
		{name: "determined chunk size", old: determined, change: func(blob *v1alpha1.Blob) { blob.Spec.ChunkSize = 8 * mib }, want: "chunkSize is immutable"},
		{name: "determined chunks number", old: determined, change: func(blob *v1alpha1.Blob) { blob.Spec.ChunksNumber = 8 }, want: "chunksNumber is immutable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := update(testStrategy, tt.old, tt.change)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("update error = %v, want %q", err, tt.want)
			}
		})
	}
}
