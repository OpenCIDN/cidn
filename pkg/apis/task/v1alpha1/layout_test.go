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
	"math"
	"math/rand/v2"
	"testing"
)

const (
	mib = 1024 * 1024
	gib = 1024 * mib
)

func TestParseResponseHeaders(t *testing.T) {
	tests := []struct {
		name              string
		headers           map[string]string
		forceAcceptRanges bool
		wantTotal         int64
		wantAcceptRanges  bool
	}{
		{name: "no headers", wantTotal: -1},
		{name: "content-length missing even when forced", headers: map[string]string{"accept-ranges": "bytes"}, forceAcceptRanges: true, wantTotal: -1},
		{name: "content-length zero", headers: map[string]string{"content-length": "0", "accept-ranges": "bytes"}, wantTotal: -1},
		{name: "content-length negative", headers: map[string]string{"content-length": "-1", "accept-ranges": "bytes"}, wantTotal: -1},
		{name: "content-length not a number", headers: map[string]string{"content-length": "abc", "accept-ranges": "bytes"}, wantTotal: -1},
		{name: "accept-ranges bytes", headers: map[string]string{"content-length": "1024", "accept-ranges": "bytes"}, wantTotal: 1024, wantAcceptRanges: true},
		{name: "accept-ranges none", headers: map[string]string{"content-length": "1024", "accept-ranges": "none"}, wantTotal: 1024},
		{name: "accept-ranges missing", headers: map[string]string{"content-length": "1024"}, wantTotal: 1024},
		{name: "forced without accept-ranges", headers: map[string]string{"content-length": "1024"}, forceAcceptRanges: true, wantTotal: 1024, wantAcceptRanges: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTotal, gotAcceptRanges := ParseResponseHeaders(tt.headers, tt.forceAcceptRanges)
			if gotTotal != tt.wantTotal || gotAcceptRanges != tt.wantAcceptRanges {
				t.Errorf("ParseResponseHeaders() = %d, %t, want %d, %t", gotTotal, gotAcceptRanges, tt.wantTotal, tt.wantAcceptRanges)
			}
		})
	}
}

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
		{name: "unknown total with range support", total: -1, acceptRanges: true, chunksNumber: 4, wantChunksNumber: 1, wantChunkSize: -1},
		{name: "unknown total with a chunk size only", total: -1, acceptRanges: true, chunkSize: 16 * mib, wantChunksNumber: 1, wantChunkSize: -1},
		{name: "minimum chunk size splits evenly", total: 300 * mib, acceptRanges: true, minimumChunkSize: 128 * mib, wantChunksNumber: 2, wantChunkSize: 150 * mib},
		{name: "minimum chunk size above the total", total: 100 * mib, acceptRanges: true, minimumChunkSize: 128 * mib, wantChunksNumber: 1, wantChunkSize: 100 * mib},
		{name: "minimum chunk size rounds the chunk size up", total: 16 * mib, acceptRanges: true, minimumChunkSize: 5 * mib, wantChunksNumber: 3, wantChunkSize: 5592406},
		{name: "minimum chunk size wins over the presets", total: 16 * mib, acceptRanges: true, minimumChunkSize: 5 * mib, chunkSize: 6 * mib, chunksNumber: 4, wantChunksNumber: 3, wantChunkSize: 5592406},
		{name: "at most 10000 chunks", total: 20000 * mib, acceptRanges: true, minimumChunkSize: 1 * mib, wantChunksNumber: 10000, wantChunkSize: 2 * mib},
		{name: "10000 chunks of 5GiB", total: 10000 * 5 * gib, acceptRanges: true, minimumChunkSize: 1, wantChunksNumber: 10000, wantChunkSize: 5 * gib},
		{name: "at most 5GiB per chunk", total: 30 * gib, acceptRanges: true, minimumChunkSize: 10 * gib, wantChunksNumber: 6, wantChunkSize: 5 * gib},
		{name: "above 10000 chunks of 5GiB tiling wins over the chunk limit", total: 10000*5*gib + 1, acceptRanges: true, minimumChunkSize: 1, wantChunksNumber: 10001, wantChunkSize: 5 * gib},
		{name: "chunks number only", total: 64 * mib, acceptRanges: true, chunksNumber: 4, wantChunksNumber: 4, wantChunkSize: 16 * mib},
		{name: "chunks number one", total: 64 * mib, acceptRanges: true, chunksNumber: 1, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "chunks number only on content up to 5MiB", total: 5 * mib, acceptRanges: true, chunksNumber: 4, wantChunksNumber: 1, wantChunkSize: 5 * mib},
		{name: "chunk size only", total: 50 * mib, acceptRanges: true, chunkSize: 16 * mib, wantChunksNumber: 4, wantChunkSize: 16 * mib},
		{name: "chunk size and chunks number", total: 64 * mib, acceptRanges: true, chunkSize: 16 * mib, chunksNumber: 4, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "no presets", total: 64 * mib, acceptRanges: true, wantChunksNumber: 1, wantChunkSize: 64 * mib},
		{name: "negative chunks number counts as absent", total: 16 * mib, acceptRanges: true, chunksNumber: -2, wantChunksNumber: 1, wantChunkSize: 16 * mib},
		{name: "negative chunk size counts as absent", total: 16 * mib, acceptRanges: true, chunkSize: -3, wantChunksNumber: 1, wantChunkSize: 16 * mib},
		{name: "negative minimum chunk size counts as absent", total: 50 * mib, acceptRanges: true, minimumChunkSize: -5, chunkSize: 16 * mib, wantChunksNumber: 4, wantChunkSize: 16 * mib},
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

func TestComputeChunkLayoutTilesTheContent(t *testing.T) {
	type input struct{ total, minimumChunkSize, chunkSize, chunksNumber int64 }
	var inputs []input
	for _, total := range []int64{1, 2, 5*mib - 1, 5 * mib, 5*mib + 1, 16 * mib, 10000 * 5 * gib} {
		for _, preset := range []int64{1, 2, 3, 4, 1000, 10000, 20000} {
			inputs = append(inputs,
				input{total: total, minimumChunkSize: preset},
				input{total: total, chunksNumber: preset},
				input{total: total, chunkSize: preset},
			)
		}
	}
	r := rand.New(rand.NewPCG(72, 0))
	// A random magnitude first makes small values as likely as large ones.
	random := func(maxBits int) int64 { return 1 + r.Int64N(int64(1)<<r.IntN(maxBits)) }
	for range 100000 {
		in := input{total: random(46)} // at most 2^45, below 10000 chunks of 5GiB
		if r.IntN(2) == 0 {
			in.minimumChunkSize = random(34)
		}
		if r.IntN(2) == 0 {
			in.chunkSize = random(34)
		}
		if r.IntN(2) == 0 {
			in.chunksNumber = random(15)
		}
		inputs = append(inputs, in)
	}

	for _, in := range inputs {
		chunksNumber, chunkSize := ComputeChunkLayout(in.total, true, in.minimumChunkSize, in.chunkSize, in.chunksNumber)
		if chunksNumber <= 1 {
			continue
		}
		if last := in.total - (chunksNumber-1)*chunkSize; last <= 0 || last > chunkSize {
			t.Errorf("ComputeChunkLayout(%+v) = %d chunks of %d, so the last chunk has %d bytes", in, chunksNumber, chunkSize, last)
		}
	}
}

func TestComputeChunkLayoutStaysPositiveWhateverThePresets(t *testing.T) {
	presets := []int64{math.MinInt64, -5 * gib, -2, -1, 0, 1, 2, 5 * mib, 5 * gib, math.MaxInt64}
	for _, total := range []int64{-1, 0, 1, 5 * mib, 5*mib + 1, 16 * mib, 10000*5*gib + 1, math.MaxInt64} {
		for _, acceptRanges := range []bool{false, true} {
			for _, minimumChunkSize := range presets {
				for _, chunkSize := range presets {
					for _, chunksNumber := range presets {
						gotChunksNumber, gotChunkSize := ComputeChunkLayout(total, acceptRanges, minimumChunkSize, chunkSize, chunksNumber)
						if gotChunksNumber < 1 || (total > 0 && gotChunkSize < 1) {
							t.Errorf("ComputeChunkLayout(%d, %t, %d, %d, %d) = %d chunks of %d", total, acceptRanges, minimumChunkSize, chunkSize, chunksNumber, gotChunksNumber, gotChunkSize)
						}
					}
				}
			}
		}
	}
}
