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

package webui

import (
	"testing"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBlobToEntry_Timestamps(t *testing.T) {
	now := metav1.Now()
	completionTime := metav1.NewTime(now.Add(5 * time.Minute))

	blob := &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-blob",
			CreationTimestamp: now,
		},
		Spec: v1alpha1.BlobSpec{
			Priority:     100,
			ChunksNumber: 10,
		},
		Status: v1alpha1.BlobStatus{
			Phase:          v1alpha1.BlobPhaseSucceeded,
			Total:          1000,
			Progress:       1000,
			CompletionTime: &completionTime,
		},
	}

	entry := blobToEntry(blob)

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set, got empty string")
	}

	if entry.CompletionTime == "" {
		t.Error("Expected CompletionTime to be set, got empty string")
	}

	// Verify the format is RFC3339
	_, err := time.Parse(time.RFC3339, entry.CreationTime)
	if err != nil {
		t.Errorf("CreationTime is not in RFC3339 format: %v", err)
	}

	_, err = time.Parse(time.RFC3339, entry.CompletionTime)
	if err != nil {
		t.Errorf("CompletionTime is not in RFC3339 format: %v", err)
	}
}

func TestBlobToEntry_NoCompletionTime(t *testing.T) {
	now := metav1.Now()

	blob := &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-blob",
			CreationTimestamp: now,
		},
		Spec: v1alpha1.BlobSpec{
			Priority:     100,
			ChunksNumber: 10,
		},
		Status: v1alpha1.BlobStatus{
			Phase:    v1alpha1.BlobPhaseRunning,
			Total:    1000,
			Progress: 500,
		},
	}

	entry := blobToEntry(blob)

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set, got empty string")
	}

	if entry.CompletionTime != "" {
		t.Errorf("Expected CompletionTime to be empty for running blob, got %s", entry.CompletionTime)
	}
}

func TestChunkToEntry_Timestamps(t *testing.T) {
	now := metav1.Now()
	completionTime := metav1.NewTime(now.Add(2 * time.Minute))

	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-chunk",
			CreationTimestamp: now,
		},
		Spec: v1alpha1.ChunkSpec{
			Priority: 100,
			Total:    500,
		},
		Status: v1alpha1.ChunkStatus{
			Phase:          v1alpha1.ChunkPhaseSucceeded,
			Progress:       500,
			CompletionTime: &completionTime,
		},
	}

	entry := chunkToEntry(chunk)

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set, got empty string")
	}

	if entry.CompletionTime == "" {
		t.Error("Expected CompletionTime to be set, got empty string")
	}

	// Verify the format is RFC3339
	_, err := time.Parse(time.RFC3339, entry.CreationTime)
	if err != nil {
		t.Errorf("CreationTime is not in RFC3339 format: %v", err)
	}

	_, err = time.Parse(time.RFC3339, entry.CompletionTime)
	if err != nil {
		t.Errorf("CompletionTime is not in RFC3339 format: %v", err)
	}
}

func TestChunkToEntry_NoCompletionTime(t *testing.T) {
	now := metav1.Now()

	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-chunk",
			CreationTimestamp: now,
		},
		Spec: v1alpha1.ChunkSpec{
			Priority: 100,
			Total:    500,
		},
		Status: v1alpha1.ChunkStatus{
			Phase:    v1alpha1.ChunkPhaseRunning,
			Progress: 250,
		},
	}

	entry := chunkToEntry(chunk)

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set, got empty string")
	}

	if entry.CompletionTime != "" {
		t.Errorf("Expected CompletionTime to be empty for running chunk, got %s", entry.CompletionTime)
	}
}
