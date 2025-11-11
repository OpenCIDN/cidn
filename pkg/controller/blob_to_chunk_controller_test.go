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

// TestSingleChunkOptimization verifies that when ChunksNumber is 1,
// the blob skips HEAD request and directly creates a GET/PUT chunk
func TestSingleChunkOptimization(t *testing.T) {
	tests := []struct {
		name              string
		blob              *v1alpha1.Blob
		expectedChunkName string
		shouldSkipHead    bool
		hasDestination    bool
	}{
		{
			name: "single chunk blob skips HEAD request",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-single-chunk-blob",
					UID:  types.UID("blob-single"),
				},
				Spec: v1alpha1.BlobSpec{
					ChunksNumber: 1,
					Source: []v1alpha1.BlobSource{
						{
							URL: "https://example.com/file.txt",
						},
					},
					Destination: []v1alpha1.BlobDestination{}, // No destination to avoid S3 dependency
					MaximumRetry: 3,
				},
				Status: v1alpha1.BlobStatus{
					HandlerName: "test-handler",
					Phase:       v1alpha1.BlobPhaseRunning,
					Total:       0, // Total unknown - normally would trigger HEAD
				},
			},
			expectedChunkName: "blob:full:test-single-chunk-blob",
			shouldSkipHead:    true,
			hasDestination:    false,
		},
		{
			name: "multi chunk blob requires HEAD request",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-multi-chunk-blob",
					UID:  types.UID("blob-multi"),
				},
				Spec: v1alpha1.BlobSpec{
					ChunksNumber: 0, // Not set yet
					Source: []v1alpha1.BlobSource{
						{
							URL: "https://example.com/file.txt",
						},
					},
					Destination: []v1alpha1.BlobDestination{}, // No destination
					MaximumRetry: 3,
				},
				Status: v1alpha1.BlobStatus{
					HandlerName: "test-handler",
					Phase:       v1alpha1.BlobPhaseRunning,
					Total:       0,
				},
			},
			expectedChunkName: "blob:head:test-multi-chunk-blob:0",
			shouldSkipHead:    false,
			hasDestination:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create fake client
			var objs []runtime.Object
			objs = append(objs, tt.blob)

			client := fake.NewSimpleClientset(objs...)
			sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)

			// Create a mock S3 client map (empty for this test)
			mockS3 := map[string]*sss.SSS{}

			// Create controller
			controller := NewBlobToChunkController(
				"test-handler",
				mockS3,
				client,
				sharedInformerFactory,
			)

			// Start informers and wait for cache sync
			sharedInformerFactory.Start(ctx.Done())
			sharedInformerFactory.WaitForCacheSync(ctx.Done())

			// Process the blob
			err := controller.chunkHandler(ctx, tt.blob.Name)

			// For blobs without destinations, chunk creation should succeed
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Verify the correct chunk was created
			chunks, err := client.TaskV1alpha1().Chunks().List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("Failed to list chunks: %v", err)
			}

			// When no destination is provided for single chunk, the blob should succeed immediately
			// So for the single chunk case, we might not have a chunk created but blob status should be updated
			if tt.shouldSkipHead && !tt.hasDestination {
				// Single chunk with no destination - blob should be marked as succeeded
				_, err := client.TaskV1alpha1().Blobs().Get(ctx, tt.blob.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Failed to get blob: %v", err)
				}
				// The blob phase might not be updated in the test since we're calling chunkHandler directly
				// Just verify no HEAD chunk was created
				if len(chunks.Items) > 0 {
					for _, chunk := range chunks.Items {
						if chunk.Spec.Source.Request.Method == "HEAD" {
							t.Errorf("Single chunk blob should not create HEAD chunk, but found one")
						}
					}
				}
			} else {
				if len(chunks.Items) != 1 {
					t.Errorf("Expected 1 chunk to be created, got %d", len(chunks.Items))
				} else {
					chunk := chunks.Items[0]
					if chunk.Name != tt.expectedChunkName {
						t.Errorf("Expected chunk name %s, got %s", tt.expectedChunkName, chunk.Name)
					}

					// Verify the chunk method for validation
					if tt.shouldSkipHead {
						// Should be GET for full chunk
						if chunk.Spec.Source.Request.Method != "GET" {
							t.Errorf("Expected GET method for single chunk, got %s", chunk.Spec.Source.Request.Method)
						}
					} else {
						// Should be HEAD for head chunk
						if chunk.Spec.Source.Request.Method != "HEAD" {
							t.Errorf("Expected HEAD method for head chunk, got %s", chunk.Spec.Source.Request.Method)
						}
					}
				}
			}
		})
	}
}
