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
	// HandlerName is the name of the handler.
	HandlerName string `json:"handlerName"`

	// Phase represents the current phase of the blob.
	Phase BlobPhase `json:"phase,omitempty"`

	// Total represents the total amount of work to be done for this blob.
	Total int64 `json:"total,omitempty"`

	// AcceptRanges represents the accept-ranges header of the response.
	AcceptRanges bool `json:"acceptRanges,omitempty"`

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

	// Retry is the number of times the blob has been retried.
	Retry int64 `json:"retry,omitempty"`

	// Conditions holds conditions for the Blob.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// BlobSpec defines the specification for Blob.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobSpec struct {
	// Source is the source of the blob.
	Source []BlobSource `json:"source"`

	// Destination is the destination of the blob.
	Destination []BlobDestination `json:"destination"`

	// Priority represents the relative importance of this blob when multiple blobs exist.
	Priority int64 `json:"priority"`

	// MinimumChunkSize represents the minimum size of each chunk when splitting the blob.
	MinimumChunkSize int64 `json:"minimumChunkSize,omitempty"`

	// ChunkSize represents the size of each chunk when splitting the blob.
	ChunkSize int64 `json:"chunkSize,omitempty"`

	// ChunksNumber represents the total number of chunks that the blob will be split into.
	ChunksNumber int64 `json:"chunksNumber,omitempty"`

	// ContentSha256 is the sha256 checksum of the blob content being verified.
	ContentSha256 string `json:"contentSha256,omitempty"`

	// MaximumRunning is the maximum number of running chunks allowed for this blob.
	// +default=2
	MaximumRunning int64 `json:"maximumRunning,omitempty"`

	// MaximumPending is the maximum number of pending chunks allowed for this blob.
	// +default=1
	MaximumPending int64 `json:"maximumPending,omitempty"`

	// MaximumRetry is the number of times the chunk has been retried.
	// +default=3
	MaximumRetry int64 `json:"maximumRetry,omitempty"`
}

// BlobSource defines the source for a blob.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobSource struct {
	// URL is the source URL of the blob.
	URL string `json:"url"`

	// BearerName is the name of the Bearer resource that should be used for authentication.
	BearerName string `json:"bearerName,omitempty"`
}

// BlobDestination defines the destination for a blob.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BlobDestination struct {
	// Name is the name of the destination.
	Name string `json:"name"`

	// Path is the filesystem path where the blob should be stored.
	Path string `json:"path"`

	// ReverifySha256 indicates whether the blob's content should be verified with SHA256 checksum.
	// If true, the blob will be verified after download using the ContentSha256 value.
	ReverifySha256 bool `json:"reverifySha256,omitempty"`

	// SkipIfExists indicates whether to skip the download if the blob already exists at the destination.
	SkipIfExists bool `json:"skipIfExists,omitempty"`
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

const (
	// BlobDisplayNameAnnotation is the annotation key for the display name of a blob.
	BlobDisplayNameAnnotation = "blob.task.opencidn.daocloud.io/display-name"
	// BlobGroupAnnotationPrefix is the annotation key prefix for group membership of a blob.
	// To add a blob to multiple groups, use separate annotation keys with this prefix followed by the group name.
	// For example: "blob.task.opencidn.daocloud.io/group/group1", "blob.task.opencidn.daocloud.io/group/group2"
	BlobGroupAnnotationPrefix = "blob.task.opencidn.daocloud.io/group/"
	// BlobGroupAnnotation is deprecated. Use BlobGroupAnnotationPrefix instead.
	// Kept for backward compatibility with single-group annotations.
	BlobGroupAnnotation = "blob.task.opencidn.daocloud.io/group"
)
