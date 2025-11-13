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

func TestBlobToEntry_WithTimestamps(t *testing.T) {
	now := metav1.Now()
	completionTime := metav1.NewTime(now.Add(5 * time.Minute))

	blob := &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-blob",
			CreationTimestamp: now,
			Annotations: map[string]string{
				v1alpha1.WebuiDisplayNameAnnotation: "Test Blob",
			},
		},
		Status: v1alpha1.BlobStatus{
			Phase:          v1alpha1.BlobPhaseSucceeded,
			Total:          1000,
			Progress:       1000,
			CompletionTime: &completionTime,
		},
	}

	entry := blobToEntry(blob)

	if entry.Name != "Test Blob" {
		t.Errorf("Expected name 'Test Blob', got '%s'", entry.Name)
	}

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set")
	}

	if entry.CompletionTime == "" {
		t.Error("Expected CompletionTime to be set")
	}

	// Verify the timestamps can be parsed
	_, err := time.Parse(time.RFC3339, entry.CreationTime)
	if err != nil {
		t.Errorf("Failed to parse CreationTime: %v", err)
	}

	_, err = time.Parse(time.RFC3339, entry.CompletionTime)
	if err != nil {
		t.Errorf("Failed to parse CompletionTime: %v", err)
	}
}

func TestChunkToEntry_WithTimestamps(t *testing.T) {
	now := metav1.Now()
	completionTime := metav1.NewTime(now.Add(2 * time.Minute))

	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-chunk",
			CreationTimestamp: now,
			Annotations: map[string]string{
				v1alpha1.WebuiDisplayNameAnnotation: "Test Chunk",
			},
		},
		Spec: v1alpha1.ChunkSpec{
			Total: 100,
		},
		Status: v1alpha1.ChunkStatus{
			Phase:          v1alpha1.ChunkPhaseSucceeded,
			Progress:       100,
			CompletionTime: &completionTime,
		},
	}

	entry := chunkToEntry(chunk)

	if entry.Name != "Test Chunk" {
		t.Errorf("Expected name 'Test Chunk', got '%s'", entry.Name)
	}

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set")
	}

	if entry.CompletionTime == "" {
		t.Error("Expected CompletionTime to be set")
	}

	// Verify the timestamps can be parsed
	_, err := time.Parse(time.RFC3339, entry.CreationTime)
	if err != nil {
		t.Errorf("Failed to parse CreationTime: %v", err)
	}

	_, err = time.Parse(time.RFC3339, entry.CompletionTime)
	if err != nil {
		t.Errorf("Failed to parse CompletionTime: %v", err)
	}
}

func TestAggregateEntries_WithTimestamps(t *testing.T) {
	now := time.Now()
	earliestTime := now.Add(-10 * time.Minute)
	latestTime := now

	entries := map[string]*entry{
		"entry1": {
			Name:           "Entry 1",
			Phase:          "Succeeded",
			CreationTime:   earliestTime.Format(time.RFC3339),
			CompletionTime: now.Add(-5 * time.Minute).Format(time.RFC3339),
			Total:          100,
			Progress:       100,
		},
		"entry2": {
			Name:           "Entry 2",
			Phase:          "Succeeded",
			CreationTime:   now.Add(-8 * time.Minute).Format(time.RFC3339),
			CompletionTime: latestTime.Format(time.RFC3339),
			Total:          200,
			Progress:       200,
		},
	}

	aggregate := aggregateEntries("test-group", entries)

	if aggregate.Name != "test-group" {
		t.Errorf("Expected name 'test-group', got '%s'", aggregate.Name)
	}

	if aggregate.CreationTime == "" {
		t.Error("Expected CreationTime to be set for aggregate")
	}

	if aggregate.CompletionTime == "" {
		t.Error("Expected CompletionTime to be set for aggregate")
	}

	// Parse and verify earliest creation time
	creationTime, err := time.Parse(time.RFC3339, aggregate.CreationTime)
	if err != nil {
		t.Errorf("Failed to parse aggregate CreationTime: %v", err)
	}

	// Allow for 2 second difference due to time formatting precision
	expectedEarliest := earliestTime
	actualEarliest := creationTime
	diff := actualEarliest.Sub(expectedEarliest)
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("Expected earliest creation time around %v, got %v (diff: %v)", expectedEarliest, actualEarliest, diff)
	}

	// Parse and verify latest completion time
	completionTime, err := time.Parse(time.RFC3339, aggregate.CompletionTime)
	if err != nil {
		t.Errorf("Failed to parse aggregate CompletionTime: %v", err)
	}

	// Allow for 2 second difference due to time formatting precision
	expectedLatest := latestTime
	actualLatest := completionTime
	diff = actualLatest.Sub(expectedLatest)
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("Expected latest completion time around %v, got %v (diff: %v)", expectedLatest, actualLatest, diff)
	}

	if aggregate.Phase != "Succeeded" {
		t.Errorf("Expected phase 'Succeeded', got '%s'", aggregate.Phase)
	}

	if aggregate.Total != 300 {
		t.Errorf("Expected total 300, got %d", aggregate.Total)
	}
}

func TestBlobToEntry_WithoutCompletionTime(t *testing.T) {
	now := metav1.Now()

	blob := &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-blob",
			CreationTimestamp: now,
			Annotations: map[string]string{
				v1alpha1.WebuiDisplayNameAnnotation: "Test Blob",
			},
		},
		Status: v1alpha1.BlobStatus{
			Phase:    v1alpha1.BlobPhaseRunning,
			Total:    1000,
			Progress: 500,
		},
	}

	entry := blobToEntry(blob)

	if entry.CreationTime == "" {
		t.Error("Expected CreationTime to be set")
	}

	if entry.CompletionTime != "" {
		t.Error("Expected CompletionTime to be empty for running blob")
	}
}
