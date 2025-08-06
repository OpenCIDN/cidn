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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ChunkKind is the kind of the Chunk resource.
	ChunkKind = "Chunk"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced

// Chunk is an API that describes the staged change of a resource
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Chunk struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec holds information about the request being evaluated.
	Spec ChunkSpec `json:"spec"`
	// Status holds status for the Chunk
	Status ChunkStatus `json:"status,omitempty"`
}

// ChunkPhase defines the phase of a Chunk
type ChunkPhase string

const (
	// ChunkPhasePending means the chunk is pending
	ChunkPhasePending ChunkPhase = "Pending"
	// ChunkPhaseRunning means the chunk is running
	ChunkPhaseRunning ChunkPhase = "Running"
	// ChunkPhaseSucceeded means the chunk has succeeded
	ChunkPhaseSucceeded ChunkPhase = "Succeeded"
	// ChunkPhaseFailed means the chunk has failed
	ChunkPhaseFailed ChunkPhase = "Failed"
	// ChunkPhaseUnknown means the chunk status is unknown
	ChunkPhaseUnknown ChunkPhase = "Unknown"
)

// ChunkStatus holds status for the Chunk
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type ChunkStatus struct {
	// HandlerName is the name of the handler.
	HandlerName string `json:"handlerName"`

	// Phase represents the current phase of the chunk.
	Phase ChunkPhase `json:"phase,omitempty"`

	// Progress is the progress of the chunk.
	Progress int64 `json:"progress,omitempty"`

	// SourceResponse is the response from the source resource
	SourceResponse *ChunkHTTPResponse `json:"sourceResponse,omitempty"`

	// ResponseBody contains the raw response body from the source resource
	ResponseBody []byte `json:"responseBody,omitempty"`

	// SourceProgress is the progress of reading the source resource
	SourceProgress int64 `json:"sourceProgress,omitempty"`

	// DestinationProgresses tracks progress of writing to destination resources
	DestinationProgresses []int64 `json:"destinationProgresses,omitempty"`

	// Sha256Partial contains sha256 hash that is partially calculated and saved
	Sha256Partial []byte `json:"sha256Partial,omitempty"`

	// Sha256 is the sha256 hash of the resource snapshot being verified.
	Sha256 string `json:"sha256,omitempty"`

	// Etag is the ETag of the resource snapshot being verified.
	Etags []string `json:"etags,omitempty"`

	// RetryCount is the number of times the chunk has been retried.
	RetryCount int64 `json:"retryCount,omitempty"`

	// Conditions holds conditions for the Chunk.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// ChunkHTTPRequest defines the HTTP request parameters for Chunk
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type ChunkHTTPRequest struct {
	// Method is the HTTP method (GET, POST, PUT, DELETE, etc)
	Method string `json:"method"`

	// URL is the target URL for the request
	URL string `json:"url"`

	// Headers contains the HTTP headers for the request
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
}

// ChunkHTTPResponse defines the expected HTTP response for validation
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type ChunkHTTPResponse struct {
	// StatusCode is the expected HTTP status code
	StatusCode int `json:"statusCode"`

	// Headers contains the HTTP headers from the response
	Headers map[string]string `json:"headers,omitempty"`
}

// ChunkHTTP defines the HTTP request/response configuration for Chunk
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type ChunkHTTP struct {
	// Request defines the HTTP request parameters
	Request ChunkHTTPRequest `json:"request"`

	// Response defines the expected HTTP response for validation
	Response ChunkHTTPResponse `json:"response"`
}

// ChunkSpec defines the specification for Chunk.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type ChunkSpec struct {
	// Source is the source of the chunk.
	Source ChunkHTTP `json:"source"`

	// Destination is the destination of the chunk.
	Destination []ChunkHTTP `json:"destination,omitempty"`

	// Priority represents the relative importance of this chunk when multiple chunks exist.
	Priority int64 `json:"priority"`

	// Total represents the total amount of work to be done for this chunk.
	Total int64 `json:"total,omitempty"`

	// ChunkIndex represents the part number in a multipart upload operation.
	// 0 means not part of a multipart upload, >0 means the part number (1-based index).
	ChunkIndex int64 `json:"chunkIndex,omitempty"`

	// ChunksNumber represents the total number of chunks that the chunk is part of in a multipart operation.
	ChunksNumber int64 `json:"chunksNumber,omitempty"`

	// Sha256PartialPreviousName indicates if this is an initial block when set to "-"
	Sha256PartialPreviousName string `json:"sha256PartialPreviousName,omitempty"`

	// Sha256 is the expected SHA256 hash of the complete content
	Sha256 string `json:"sha256,omitempty"`

	// RetryCount is the number of times the chunk has been retried.
	// +default=5
	RetryCount int64 `json:"retryCount,omitempty"`

	// InlineResponseBody indicates whether the response body should be inlined in the status
	InlineResponseBody bool `json:"inlineResponseBody,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ChunkList contains a list of Chunk
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type ChunkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Chunk `json:"items"`
}
