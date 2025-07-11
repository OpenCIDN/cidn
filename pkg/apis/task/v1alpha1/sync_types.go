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
	// SyncKind is the kind of the Sync resource.
	SyncKind = "Sync"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus

// Sync is an API that describes the staged change of a resource
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Sync struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec holds information about the request being evaluated.
	Spec SyncSpec `json:"spec"`
	// Status holds status for the Sync
	Status SyncStatus `json:"status,omitempty"`
}

// SyncPhase defines the phase of a Sync
type SyncPhase string

const (
	// SyncPhasePending means the sync is pending
	SyncPhasePending SyncPhase = "Pending"
	// SyncPhaseRunning means the sync is running
	SyncPhaseRunning SyncPhase = "Running"
	// SyncPhaseSucceeded means the sync has succeeded
	SyncPhaseSucceeded SyncPhase = "Succeeded"
	// SyncPhaseFailed means the sync has failed
	SyncPhaseFailed SyncPhase = "Failed"
	// SyncPhaseUnknown means the sync status is unknown
	SyncPhaseUnknown SyncPhase = "Unknown"
)

// SyncStatus holds status for the Sync
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type SyncStatus struct {
	// Phase represents the current phase of the sync.
	Phase SyncPhase `json:"phase,omitempty"`

	// Progress is the progress of the sync.
	Progress int64 `json:"progress,omitempty"`

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

	// Conditions holds conditions for the Sync.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// LastTime is the timestamp when the sync was last performed
	// +optional
	LastTime *metav1.Time `json:"lastTime,omitempty"`
}

// SyncHTTPRequest defines the HTTP request parameters for Sync
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type SyncHTTPRequest struct {
	// Method is the HTTP method (GET, POST, PUT, DELETE, etc)
	Method string `json:"method"`

	// URL is the target URL for the request
	URL string `json:"url"`

	// Headers contains the HTTP headers for the request
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
}

// SyncHTTPResponse defines the expected HTTP response for validation
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type SyncHTTPResponse struct {
	// StatusCode is the expected HTTP status code
	StatusCode int `json:"statusCode"`
}

// SyncHTTP defines the HTTP request/response configuration for Sync
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type SyncHTTP struct {
	// Request defines the HTTP request parameters
	Request SyncHTTPRequest `json:"request"`

	// Response defines the expected HTTP response for validation
	Response SyncHTTPResponse `json:"response"`
}

// SyncSpec defines the specification for Sync.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type SyncSpec struct {
	// HandlerName is the name of the handler.
	HandlerName string `json:"handlerName"`

	// Source is the source of the sync.
	Source SyncHTTP `json:"source"`

	// Destination is the destination of the sync.
	Destination []SyncHTTP `json:"destination"`

	// Priority represents the relative importance of this sync when multiple syncs exist.
	Priority int64 `json:"priority"`

	// Total represents the total amount of work to be done for this sync.
	Total int64 `json:"total"`

	// ChunkIndex represents the part number in a multipart upload operation.
	// 0 means not part of a multipart upload, >0 means the part number (1-based index).
	ChunkIndex int64 `json:"chunkIndex,omitempty"`

	// ChunksNumber represents the total number of chunks that the sync is part of in a multipart operation.
	ChunksNumber int64 `json:"chunksNumber,omitempty"`

	// Sha256PartialPreviousName indicates if this is an initial block when set to "-"
	Sha256PartialPreviousName string `json:"sha256PartialPreviousName,omitempty"`

	// Sha256 is the expected SHA256 hash of the complete content
	Sha256 string `json:"sha256,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SyncList contains a list of Sync
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type SyncList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Sync `json:"items"`
}
