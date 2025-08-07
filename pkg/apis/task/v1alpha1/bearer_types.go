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
	// BearerKind is the kind of the Bearer resource.
	BearerKind = "Bearer"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced

// Bearer is an API that describes the staged change of a resource
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Bearer struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec holds information about the request being evaluated.
	Spec BearerSpec `json:"spec"`
	// Status holds status for the Bearer
	Status BearerStatus `json:"status,omitempty"`
}

// BearerPhase defines the phase of a Bearer
type BearerPhase string

const (
	// BearerPhasePending means the manifest is pending
	BearerPhasePending BearerPhase = "Pending"
	// BearerPhaseRunning means the manifest is running
	BearerPhaseRunning BearerPhase = "Running"
	// BearerPhaseSucceeded means the manifest has succeeded
	BearerPhaseSucceeded BearerPhase = "Succeeded"
	// BearerPhaseFailed means the manifest has failed
	BearerPhaseFailed BearerPhase = "Failed"
	// BearerPhaseUnknown means the manifest status is unknown
	BearerPhaseUnknown BearerPhase = "Unknown"
)

// BearerStatus holds status for the Bearer
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BearerStatus struct {
	// HandlerName is the name of the handler.
	HandlerName string `json:"handlerName"`

	// Phase represents the current phase of the manifest.
	Phase BearerPhase `json:"phase,omitempty"`

	// TokenInfo contains information about the authentication token
	TokenInfo *BearerTokenInfo `json:"tokenInfo,omitempty"`

	// Retry is the number of times the manifest has been retried.
	Retry int64 `json:"retry,omitempty"`

	// Conditions holds conditions for the Bearer.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// BearerSpec defines the specification for Bearer.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BearerSpec struct {
	// URL is the URL that is being checked for authorization.
	URL string `json:"url"`

	// Priority represents the relative importance of this manifest when multiple manifests exist.
	Priority int64 `json:"priority,omitempty"`

	// MaximumRetry is the number of times the chunk has been retried.
	// +default=3
	MaximumRetry int64 `json:"maximumRetry,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BearerList contains a list of Bearer
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BearerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bearer `json:"items"`
}

// BearerTokenInfo contains information about an authentication token
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type BearerTokenInfo struct {
	// Token is the actual authentication token string
	Token string `json:"token"`

	// ExpiresIn is the duration in seconds until the token expires
	ExpiresIn int64 `json:"expires_in,omitempty"`

	// IssuedAt is the timestamp when the token was issued
	IssuedAt metav1.Time `json:"issued_at,omitempty"`
}
