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

func TestDeleteChunksInNonFinalStates(t *testing.T) {
	tests := []struct {
		name                string
		blob                *v1alpha1.Blob
		chunks              []*v1alpha1.Chunk
		expectedToBeDeleted []string
		expectedToRemain    []string
	}{
		{
			name: "delete running and pending chunks only",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					UID:  types.UID("blob-123"),
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhaseFailed,
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
						Name: "chunk-unknown",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-123",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseUnknown,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "chunk-succeeded",
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
			expectedToBeDeleted: []string{"chunk-running", "chunk-pending", "chunk-unknown"},
			expectedToRemain:    []string{"chunk-succeeded", "chunk-failed"},
		},
		{
			name: "no chunks to delete when all are in final states",
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
						Name: "chunk-succeeded-1",
						Labels: map[string]string{
							BlobUIDLabelKey: "blob-456",
						},
					},
					Status: v1alpha1.ChunkStatus{
						Phase: v1alpha1.ChunkPhaseSucceeded,
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
			expectedToBeDeleted: []string{},
			expectedToRemain:    []string{"chunk-succeeded-1", "chunk-failed-1"},
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
			controller := NewBlobFromChunkController(
				"test-handler",
				map[string]*sss.SSS{}, // Use empty map instead of nil
				client,
				sharedInformerFactory,
			)

			// Start informers and wait for cache sync
			sharedInformerFactory.Start(ctx.Done())
			sharedInformerFactory.WaitForCacheSync(ctx.Done())

			// Call the function under test
			controller.deleteChunksInNonFinalStates(ctx, tt.blob)

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
		})
	}
}

func TestForceAcceptRanges(t *testing.T) {
	tests := []struct {
		name                     string
		blob                     *v1alpha1.Blob
		chunk                    *v1alpha1.Chunk
		expectedAcceptRanges     bool
		expectAcceptRangesInResp bool
	}{
		{
			name: "ForceAcceptRanges set to true overrides server response",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob-force-true",
				},
				Spec: v1alpha1.BlobSpec{
					ForceAcceptRanges: true,
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhasePending,
				},
			},
			chunk: &v1alpha1.Chunk{
				ObjectMeta: metav1.ObjectMeta{
					Name: "blob:head:test-blob-force-true:0",
				},
				Status: v1alpha1.ChunkStatus{
					Phase: v1alpha1.ChunkPhaseSucceeded,
					SourceResponse: &v1alpha1.ChunkHTTPResponse{
						Headers: map[string]string{
							"content-length": "1024",
							"accept-ranges":  "none", // Server says it doesn't support ranges
						},
					},
				},
			},
			expectedAcceptRanges:     true,
			expectAcceptRangesInResp: false,
		},
		{
			name: "ForceAcceptRanges false respects server response - accepts ranges",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob-force-false",
				},
				Spec: v1alpha1.BlobSpec{
					ForceAcceptRanges: false,
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhasePending,
				},
			},
			chunk: &v1alpha1.Chunk{
				ObjectMeta: metav1.ObjectMeta{
					Name: "blob:head:test-blob-force-false:0",
				},
				Status: v1alpha1.ChunkStatus{
					Phase: v1alpha1.ChunkPhaseSucceeded,
					SourceResponse: &v1alpha1.ChunkHTTPResponse{
						Headers: map[string]string{
							"content-length": "2048",
							"accept-ranges":  "bytes",
						},
					},
				},
			},
			expectedAcceptRanges:     true,
			expectAcceptRangesInResp: true,
		},
		{
			name: "ForceAcceptRanges false respects server response - no ranges",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob-no-force",
				},
				Spec: v1alpha1.BlobSpec{
					ForceAcceptRanges: false,
				},
				Status: v1alpha1.BlobStatus{
					Phase: v1alpha1.BlobPhasePending,
				},
			},
			chunk: &v1alpha1.Chunk{
				ObjectMeta: metav1.ObjectMeta{
					Name: "blob:head:test-blob-no-force:0",
				},
				Status: v1alpha1.ChunkStatus{
					Phase: v1alpha1.ChunkPhaseSucceeded,
					SourceResponse: &v1alpha1.ChunkHTTPResponse{
						Headers: map[string]string{
							"content-length": "512",
							"accept-ranges":  "none",
						},
					},
				},
			},
			expectedAcceptRanges:     false,
			expectAcceptRangesInResp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create fake client
			client := fake.NewSimpleClientset(tt.chunk)
			sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)

			// Create controller
			controller := NewBlobFromChunkController(
				"test-handler",
				map[string]*sss.SSS{},
				client,
				sharedInformerFactory,
			)

			// Start informers and wait for cache sync
			sharedInformerFactory.Start(ctx.Done())
			sharedInformerFactory.WaitForCacheSync(ctx.Done())

			// Call the function under test
			err := controller.fromHeadChunk(ctx, tt.blob)
			if err != nil {
				t.Fatalf("fromHeadChunk failed: %v", err)
			}

			// Verify the AcceptRanges field is set correctly
			if tt.blob.Status.AcceptRanges != tt.expectedAcceptRanges {
				t.Errorf("Expected AcceptRanges to be %v, got %v", tt.expectedAcceptRanges, tt.blob.Status.AcceptRanges)
			}

			// Verify server response was as expected
			serverAcceptsRanges := tt.chunk.Status.SourceResponse.Headers["accept-ranges"] == "bytes"
			if serverAcceptsRanges != tt.expectAcceptRangesInResp {
				t.Errorf("Expected server accept-ranges header to indicate %v, got %v", tt.expectAcceptRangesInResp, serverAcceptsRanges)
			}
		})
	}
}
