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
	"strconv"
)

// ParseResponseHeaders reads the total size (-1 if unknown) and range support from lowercase response headers.
func ParseResponseHeaders(headers map[string]string, forceAcceptRanges bool) (total int64, acceptRanges bool) {
	total, _ = strconv.ParseInt(headers["content-length"], 10, 64)
	if total <= 0 {
		return -1, false
	}
	return total, forceAcceptRanges || headers["accept-ranges"] == "bytes"
}

// ComputeChunkLayout returns how many chunks of which size a blob of total bytes is split into.
func ComputeChunkLayout(total int64, acceptRanges bool, minimumChunkSize, presetChunkSize, presetChunksNumber int64) (chunksNumber, chunkSize int64) {
	switch {
	case !acceptRanges || total <= 0:
		return 1, total
	case minimumChunkSize != 0:
		chunksNumber = getChunksNumberByMinimumChunkSize(total, minimumChunkSize)
		chunkSize = getChunkSize(total, chunksNumber)
	case presetChunkSize == 0 && presetChunksNumber != 0:
		chunksNumber, chunkSize = presetChunksNumber, getChunkSize(total, presetChunksNumber)
	case presetChunkSize != 0 && presetChunksNumber == 0:
		chunksNumber, chunkSize = getChunksNumber(total, presetChunkSize), presetChunkSize
	default:
		return 1, total
	}
	if chunksNumber > 1 {
		// More than one chunk must tile the content exactly.
		chunksNumber = getChunksNumber(total, chunkSize)
	}
	return chunksNumber, chunkSize
}

func getChunksNumberByMinimumChunkSize(total, minimumChunkSize int64) int64 {
	chunksNumber := total / minimumChunkSize
	if chunksNumber <= 1 {
		return 1
	}
	if chunksNumber >= 10000 {
		return 10000
	}
	return chunksNumber
}

func getChunkSize(total, chunksNumber int64) int64 {
	if total <= 5*1024*1024 { // <= 5MiB
		return total
	}
	chunkSize := total / chunksNumber
	if total%chunksNumber != 0 {
		chunkSize++
	}

	if chunkSize >= 5*1024*1024*1024 { // <= 5GiB
		return 5 * 1024 * 1024 * 1024
	}
	return chunkSize
}

func getChunksNumber(total, chunkSize int64) int64 {
	if chunkSize == 0 {
		return 1
	}
	if chunkSize >= total {
		return 1
	}

	count := total / chunkSize
	if total%chunkSize != 0 {
		count++
	}
	return count
}
