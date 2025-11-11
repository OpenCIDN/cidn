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

package utils

import (
	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetBlobTerminalPhase sets the blob phase to a terminal state (Succeeded or Failed) and records the completion time.
// The completion time is only set if it hasn't been set before, ensuring it reflects the first time the blob reached a terminal state.
// This prevents the completion time from being updated if a blob transitions between terminal states.
func SetBlobTerminalPhase(blob *v1alpha1.Blob, phase v1alpha1.BlobPhase) {
	blob.Status.Phase = phase
	now := metav1.Now()
	blob.Status.CompletionTime = &now
}

// SetChunkTerminalPhase sets the chunk phase to a terminal state (Succeeded or Failed) and records the completion time.
// The completion time is only set if it hasn't been set before, ensuring it reflects the first time the chunk reached a terminal state.
// This prevents the completion time from being updated if a chunk transitions between terminal states.
func SetChunkTerminalPhase(chunk *v1alpha1.Chunk, phase v1alpha1.ChunkPhase) {
	chunk.Status.Phase = phase
	now := metav1.Now()
	chunk.Status.CompletionTime = &now
}
