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
	// MultipartKind is the kind of the Multipart resource.
	MultipartKind = "Multipart"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced

// Multipart is an API that describes the staged change of a resource
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Multipart struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// UploadIDs holds the list of upload IDs for multipart uploads
	UploadIDs []string `json:"uploadIDs,omitempty"`

	// UploadEtags holds the etags of uploaded parts for multipart uploads
	UploadEtags []UploadEtags `json:"uploadEtags,omitempty"`
}

// UploadEtags holds the etag information for uploaded parts of a multipart upload
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type UploadEtags struct {
	Size  int64    `json:"size,omitempty"`
	Etags []string `json:"etags,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MultipartList contains a list of Multipart
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type MultipartList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Multipart `json:"items"`
}
