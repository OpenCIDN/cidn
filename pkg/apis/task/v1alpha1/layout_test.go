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

package v1alpha1

import (
	"testing"
)

const (
	mib = 1024 * 1024
	gib = 1024 * mib
)

func TestComputeChunkLayout(t *testing.T) {
	tests := []struct {
		name             string
		total            int64
		acceptRanges     bool
		minimumChunkSize int64
		chunkSize        int64
		chunksNumber     int64
		wantChunksNumber int64
		wantChunkSize    int64
	}{
		{name: "not chunkable", total: 300 * mib, minimumChunkSize: 128 * mib, chunkSize: 16 * mib, wantChunksNumber: 1, wantChunkSize: 300 * mib},
		{name: "unknown total", total: -1, minimumChunkSize: 128 * mib, wantChunksNumber: 1, wantChunkSize: -1},
		{name: "minimum chunk size splits evenly", total: 300 * mib, acceptRanges: true, minimumChunkSize: 128 * mib, wantChunksNumber: 2, wantChunkSize: 150 * mib},
		{name: "minimum chunk size above the total", total: 100 * mib, acceptRanges: true, minimumChunkSize: 128 * mib, wantChunksNumber: 1, wantChunkSize: 100 * mib},
		{name: "minimum chunk size rounds the chunk size up", total: 16 * mib, acceptRanges: true, minimumChunkSize: 5 * mib, wantChunksNumber: 3, wantChunkSize: 5592406},
		{name: "minimum chunk size wins over the presets", total: 16 * mib, acceptRanges: true, minimumChunkSize: 5 * mib, chunkSize: 6 * mib, chunksNumber: 4, wantChunksNumber: 3, wantChunkSize: 5592406},
		{name: "at most 10000 chunks", total: 20000 * mib, acceptRanges: true, minimumChunkSize: 1 * mib, wantChunksNumber: 10000, wantChunkSize: 2 * mib},
		{name: "10000 chunks of 5GiB", total: 10000 * 5 * gib, acceptRanges: true, minimumChunkSize: 1, wantChunksNumber: 10000, wantChunkSize: 5 * gib},
		{name: "chunks number only", total: 64 * mib, acceptRanges: true, chunksNumber: 4, wantChunksNumber: 4, wantChunkSize: 16 * mib},
		{name: "chunks number one", total: 64 * mib, acceptRanges: true, chunksNumber: 1, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "chunk size only", total: 50 * mib, acceptRanges: true, chunkSize: 16 * mib, wantChunksNumber: 4, wantChunkSize: 16 * mib},
		{name: "chunk size and chunks number", total: 64 * mib, acceptRanges: true, chunkSize: 16 * mib, chunksNumber: 4, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "no presets", total: 64 * mib, acceptRanges: true, wantChunksNumber: 1, wantChunkSize: 64 * mib},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotChunksNumber, gotChunkSize := ComputeChunkLayout(tt.total, tt.acceptRanges, tt.minimumChunkSize, tt.chunkSize, tt.chunksNumber)
			if gotChunksNumber != tt.wantChunksNumber || gotChunkSize != tt.wantChunkSize {
				t.Errorf("ComputeChunkLayout() = %d chunks of %d, want %d chunks of %d", gotChunksNumber, gotChunkSize, tt.wantChunksNumber, tt.wantChunkSize)
			}
		})
	}
}
