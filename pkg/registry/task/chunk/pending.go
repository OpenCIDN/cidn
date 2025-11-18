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

package chunk

import (
	"context"
	"math/rand"
	"sort"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

// PendingREST implements the REST endpoint for fetching pending chunks
type PendingREST struct {
	store *REST
}

// NewPendingREST creates a new PendingREST instance
func NewPendingREST(store *REST) *PendingREST {
	return &PendingREST{store: store}
}

var _ rest.Lister = &PendingREST{}
var _ rest.Scoper = &PendingREST{}

// New returns an empty ChunkList
func (r *PendingREST) New() runtime.Object {
	return &v1alpha1.ChunkList{}
}

// NewList returns a new ChunkList
func (r *PendingREST) NewList() runtime.Object {
	return &v1alpha1.ChunkList{}
}

// NamespaceScoped returns false as chunks are cluster-scoped
func (r *PendingREST) NamespaceScoped() bool {
	return false
}

// Destroy cleans up resources
func (r *PendingREST) Destroy() {
	// Nothing to destroy
}

// List returns all pending chunks, sorted by priority, retry count, and creation time
func (r *PendingREST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	// List all chunks with field selector for pending chunks
	if options == nil {
		options = &metainternalversion.ListOptions{}
	}
	
	// Set field selector to filter for pending chunks
	fieldSelector, err := fields.ParseSelector("status.handlerName=,status.phase=Pending")
	if err != nil {
		return nil, err
	}
	options.FieldSelector = fieldSelector
	
	obj, err := r.store.List(ctx, options)
	if err != nil {
		return nil, err
	}

	chunkList, ok := obj.(*v1alpha1.ChunkList)
	if !ok {
		return obj, nil
	}

	// Sort and shuffle the chunks
	pendingChunks := sortAndShufflePendingChunks(chunkList.Items)
	chunkList.Items = pendingChunks

	return chunkList, nil
}

// ConvertToTable converts the chunk list to a table for kubectl
func (r *PendingREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// sortAndShufflePendingChunks sorts chunks by priority, retry count, and creation time, then shuffles them
func sortAndShufflePendingChunks(chunks []v1alpha1.Chunk) []v1alpha1.Chunk {
	if len(chunks) == 0 {
		return chunks
	}

	// Filter for truly pending chunks (empty handler name and pending phase)
	var pendingChunks []v1alpha1.Chunk
	for _, chunk := range chunks {
		if chunk.Status.HandlerName == "" && chunk.Status.Phase == v1alpha1.ChunkPhasePending {
			pendingChunks = append(pendingChunks, chunk)
		}
	}

	// Sort by weight (descending) and creation time (ascending)
	sort.Slice(pendingChunks, func(i, j int) bool {
		a := &pendingChunks[i]
		b := &pendingChunks[j]
		if a.Spec.Priority != b.Spec.Priority {
			return a.Spec.Priority > b.Spec.Priority
		}

		if a.Status.Retry != b.Status.Retry {
			return a.Status.Retry < b.Status.Retry
		}

		atime := a.CreationTimestamp.Time
		if a.Status.CompletionTime != nil {
			atime = a.Status.CompletionTime.Time
		}
		btime := b.CreationTimestamp.Time
		if b.Status.CompletionTime != nil {
			btime = b.Status.CompletionTime.Time
		}

		return atime.Before(btime)
	})

	return shuffleChunks(pendingChunks)
}

// shuffleChunks shuffles the chunks to a certain degree to reduce conflicts
func shuffleChunks(chunks []v1alpha1.Chunk) []v1alpha1.Chunk {
	n := len(chunks)
	if n <= 1 {
		return chunks
	}

	// Define a shuffle range (e.g., 25% of the list size)
	shuffleRange := n / 4
	if shuffleRange < 1 {
		shuffleRange = 1
	}

	// Shuffle within the defined range
	for i := 0; i < n; i++ {
		j := i + rand.Intn(shuffleRange)
		if j >= n {
			j = n - 1
		}
		chunks[i], chunks[j] = chunks[j], chunks[i]
	}

	return chunks
}
