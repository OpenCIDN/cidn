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

package controller

import (
	"context"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/wzshiming/sss"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestCleanupBlobPreservesSucceededChunk(t *testing.T) {
	tests := []struct {
		name                string
		blob                *v1alpha1.Blob
		chunks              []*v1alpha1.Chunk
		expectedToBeDeleted []string
		expectedToRemain    []string
	}{
		{
			name: "preserve at least one succeeded chunk",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					UID:  types.UID("blob-123"),
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhaseSucceeded,
				},
			},
			chunks: []*v1alpha1.Chunk{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-running",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-123",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-pending",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-123",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhasePending,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-succeeded-1",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-123",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseSucceeded,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-succeeded-2",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-123",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseSucceeded,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-failed",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-123",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseFailed,
					},
				},
			},
			expectedToBeDeleted: []string{"chunk-running", "chunk-pending", "chunk-succeeded-2", "chunk-failed"},
			expectedToRemain:    []string{"chunk-succeeded-1"}, // At least one succeeded chunk is preserved
		},
		{
			name: "delete all chunks when no succeeded chunks exist",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob-2",
					UID:  types.UID("blob-456"),
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhaseFailed,
				},
			},
			chunks: []*v1alpha1.Chunk{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-running-1",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-456",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-pending-1",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-456",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhasePending,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-failed-1",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-456",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseFailed,
					},
				},
			},
			expectedToBeDeleted: []string{"chunk-running-1", "chunk-pending-1", "chunk-failed-1"},
			expectedToRemain:    []string{},
		},
		{
			name: "preserve only one succeeded chunk when multiple exist",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob-3",
					UID:  types.UID("blob-789"),
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhaseSucceeded,
				},
			},
			chunks: []*v1alpha1.Chunk{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-succeeded-a",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-789",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseSucceeded,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-succeeded-b",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-789",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseSucceeded,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-succeeded-c",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-789",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseSucceeded,
					},
				},
			},
			expectedToBeDeleted: []string{"chunk-succeeded-b", "chunk-succeeded-c"},
			expectedToRemain:    []string{"chunk-succeeded-a"}, // First succeeded chunk is preserved
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create fake client with chunks
			var objs []runtime.Object
			for _, chunk := range tt.chunks {
				objs = append(objs, chunk)
			}

			client := fake.NewSimpleClientset(objs...)
			sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)

			// Create controller
			controller := NewBlobToChunkController(
				"test-handler",
				map[string]*sss.SSS{}, // Empty map since this test doesn't require S3 functionality
				client,
				sharedInformerFactory,
			)

			// Start informers and wait for cache sync
			sharedInformerFactory.Start(ctx.Done())
			sharedInformerFactory.WaitForCacheSync(ctx.Done())

			// Call the function under test
			controller.cleanupBlob(tt.blob)

			// Verify deleted chunks
			for _, chunkName := range tt.expectedToBeDeleted {
				_, err := client.TaskV1alpha1().Chunks().Get(ctx, chunkName, metav1.GetOptions{})
				if err == nil {
					t.Errorf("Expected chunk %s to be deleted, but it still exists", chunkName)
				}
			}

			// Verify remaining chunks
			for _, chunkName := range tt.expectedToRemain {
				_, err := client.TaskV1alpha1().Chunks().Get(ctx, chunkName, metav1.GetOptions{})
				if err != nil {
					t.Errorf("Expected chunk %s to remain, but got error: %v", chunkName, err)
				}
			}

			// Verify at least one succeeded chunk is preserved if any existed
			remainingChunks, err := client.TaskV1alpha1().Chunks().List(ctx, metav1.ListOptions{
				LabelSelector: BlobUIDLabelKey + "=" + string(tt.blob.UID),
			})
			if err != nil {
				t.Fatalf("Failed to list chunks: %v", err)
			}

			hasSucceededChunk := false
			for _, chunk := range tt.chunks {
				if chunk.Status.Phase == v1alpha1.ChunkPhaseSucceeded {
					hasSucceededChunk = true
					break
				}
			}

			if hasSucceededChunk {
				foundSucceeded := false
				for _, chunk := range remainingChunks.Items {
					if chunk.Status.Phase == v1alpha1.ChunkPhaseSucceeded {
						foundSucceeded = true
						break
					}
				}
				if !foundSucceeded {
					t.Error("Expected at least one succeeded chunk to be preserved, but none found")
				}
			}
		})
	}
}
