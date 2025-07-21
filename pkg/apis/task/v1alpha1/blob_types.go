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
	// BlobKind is the kind of the Blob resource.
	BlobKind = "Blob"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus

// Blob is an API that describes the staged change of a resource
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Blob struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec holds information about the request being evaluated.
	Spec BlobSpec `json:"spec"`
	// Status holds status for the Blob
	Status BlobStatus `json:"status,omitempty"`
}

// BlobPhase defines the phase of a Blob
type BlobPhase string

const (
	// BlobPhasePending means the blob is pending
	BlobPhasePending BlobPhase = "Pending"
	// BlobPhaseRunning means the blob is running
	BlobPhaseRunning BlobPhase = "Running"
	// BlobPhaseSucceeded means the blob has succeeded
	BlobPhaseSucceeded BlobPhase = "Succeeded"
	// BlobPhaseFailed means the blob has failed
	BlobPhaseFailed BlobPhase = "Failed"
	// BlobPhaseUnknown means the blob status is unknown
	BlobPhaseUnknown BlobPhase = "Unknown"
)

// BlobStatus holds status for the Blob
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobStatus struct {
	// Phase represents the current phase of the blob.
	Phase BlobPhase `json:"phase,omitempty"`

	// Progress is the progress of the blob.
	Progress int64 `json:"progress,omitempty"`

	// PendingChunks is the number of pending chunks for this blob.
	PendingChunks int64 `json:"pendingChunks,omitempty"`

	// RunningChunks is the number of running chunks for this blob.
	RunningChunks int64 `json:"runningChunks,omitempty"`

	// SucceededChunks is the number of succeeded chunks for this blob.
	SucceededChunks int64 `json:"succeededChunks,omitempty"`

	// FailedChunks is the number of failed chunks for this blob.
	FailedChunks int64 `json:"failedChunks,omitempty"`

	// UploadIDs holds the list of upload IDs for multipart uploads
	UploadIDs []string `json:"uploadIDs,omitempty"`

	// UploadEtags holds the etags of uploaded parts for multipart uploads
	UploadEtags []UploadEtag `json:"uploadEtags,omitempty"`

	// RetryCount is the number of times the blob has been retried.
	RetryCount int64 `json:"retryCount,omitempty"`

	// Conditions holds conditions for the Blob.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// UploadEtag holds the etag information for uploaded parts of a multipart upload
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type UploadEtag struct {
	Size  int64    `json:"size,omitempty"`
	Etags []string `json:"etags,omitempty"`
}

// BlobSpec defines the specification for Blob.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobSpec struct {
	// HandlerName is the name of the handler.
	HandlerName string `json:"handlerName"`

	// Source is the source of the blob.
	Source string `json:"source"`

	// Destination is the destination of the blob.
	Destination []BlobDestination `json:"destination"`

	// Priority represents the relative importance of this blob when multiple blobs exist.
	Priority int64 `json:"priority,omitempty"`

	// Total represents the total amount of work to be done for this blob.
	Total int64 `json:"total"`

	// MinimumChunkSize represents the minimum size of each chunk when splitting the blob.
	MinimumChunkSize int64 `json:"minimumChunkSize,omitempty"`

	// ChunkSize represents the size of each chunk when splitting the blob.
	ChunkSize int64 `json:"chunkSize,omitempty"`

	// ChunksNumber represents the total number of chunks that the blob will be split into.
	ChunksNumber int64 `json:"chunksNumber,omitempty"`

	// ContentSha256 is the sha256 checksum of the blob content being verified.
	ContentSha256 string `json:"contentSha256,omitempty"`

	// SourceEtag is the ETag of the blob content being verified.
	SourceEtag string `json:"sourceEtag,omitempty"`

	// MaximumParallelism is the maximum number of syncs allowed for this blob.
	// +default=2
	MaximumParallelism int64 `json:"maximumParallelism,omitempty"`

	// RetryCount is the number of times the sync has been retried.
	// +default=5
	RetryCount int64 `json:"retryCount,omitempty"`
}

// BlobDestination defines the destination for a blob.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobDestination struct {
	// Name is the name of the destination.
	Name string `json:"name"`

	// Path is the filesystem path where the blob should be stored.
	Path string `json:"path"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BlobList contains a list of Blob
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Blob `json:"items"`
}
