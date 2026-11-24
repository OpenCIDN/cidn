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
	"encoding/json"
	"testing"
)

func TestChunkSpecJSONCarriesOnlyTheFirstPartFieldsSet(t *testing.T) {
	const fields = `"source":{"request":{"method":"","url":""},"response":{"statusCode":0}},"priority":0`
	tests := []struct {
		name string
		spec ChunkSpec
		want string
	}{
		{name: "no first part", spec: ChunkSpec{}, want: `{` + fields + `}`},
		{name: "empty first part", spec: ChunkSpec{FirstPart: &ChunkFirstPart{}}, want: `{` + fields + `,"firstPart":{}}`},
		{
			name: "full first part",
			spec: ChunkSpec{FirstPart: &ChunkFirstPart{MinimumChunkSize: 1, ChunkSize: 2, ChunksNumber: 3, ForceAcceptRanges: true, ExistingSizes: []int64{4, 0}}},
			want: `{` + fields + `,"firstPart":{"minimumChunkSize":1,"chunkSize":2,"chunksNumber":3,"forceAcceptRanges":true,"existingSizes":[4,0]}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.spec)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}
