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

package webui

import (
	"reflect"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCleanBlobForWebUI_Tags(t *testing.T) {
	tests := []struct {
		name     string
		blob     *v1alpha1.Blob
		wantTags []string
	}{
		{
			name: "blob with single tag",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					Annotations: map[string]string{
						v1alpha1.BlobTagsAnnotation: "production",
					},
				},
			},
			wantTags: []string{"production"},
		},
		{
			name: "blob with multiple tags",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					Annotations: map[string]string{
						v1alpha1.BlobTagsAnnotation: "production, critical, backup",
					},
				},
			},
			wantTags: []string{"production", "critical", "backup"},
		},
		{
			name: "blob with tags with extra whitespace",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					Annotations: map[string]string{
						v1alpha1.BlobTagsAnnotation: "  production  ,  critical  ,  backup  ",
					},
				},
			},
			wantTags: []string{"production", "critical", "backup"},
		},
		{
			name: "blob with no tags annotation",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-blob",
					Annotations: map[string]string{},
				},
			},
			wantTags: nil,
		},
		{
			name: "blob with empty tags annotation",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					Annotations: map[string]string{
						v1alpha1.BlobTagsAnnotation: "",
					},
				},
			},
			wantTags: nil,
		},
		{
			name: "blob with tags containing empty entries",
			blob: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-blob",
					Annotations: map[string]string{
						v1alpha1.BlobTagsAnnotation: "production,,critical,,",
					},
				},
			},
			wantTags: []string{"production", "critical"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleaned := cleanBlobForWebUI(tt.blob)
			if !reflect.DeepEqual(cleaned.Tags, tt.wantTags) {
				t.Errorf("cleanBlobForWebUI() Tags = %v, want %v", cleaned.Tags, tt.wantTags)
			}
		})
	}
}

func TestAggregateBlobs_Tags(t *testing.T) {
	tests := []struct {
		name      string
		groupName string
		blobs     map[string]*v1alpha1.Blob
		wantTags  []string
	}{
		{
			name:      "single blob with tags",
			groupName: "test-group",
			blobs: map[string]*v1alpha1.Blob{
				"blob1": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "blob1",
						Annotations: map[string]string{
							v1alpha1.BlobTagsAnnotation: "production, critical",
						},
					},
					Spec: v1alpha1.BlobSpec{
						ChunksNumber: 10,
					},
					Status: v1alpha1.BlobStatus{
						Phase: v1alpha1.BlobPhaseRunning,
					},
				},
			},
			wantTags: []string{"critical", "production"},
		},
		{
			name:      "multiple blobs with overlapping tags",
			groupName: "test-group",
			blobs: map[string]*v1alpha1.Blob{
				"blob1": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "blob1",
						Annotations: map[string]string{
							v1alpha1.BlobTagsAnnotation: "production, critical",
						},
					},
					Spec: v1alpha1.BlobSpec{
						ChunksNumber: 10,
					},
					Status: v1alpha1.BlobStatus{
						Phase: v1alpha1.BlobPhaseRunning,
					},
				},
				"blob2": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "blob2",
						Annotations: map[string]string{
							v1alpha1.BlobTagsAnnotation: "production, backup",
						},
					},
					Spec: v1alpha1.BlobSpec{
						ChunksNumber: 5,
					},
					Status: v1alpha1.BlobStatus{
						Phase: v1alpha1.BlobPhaseRunning,
					},
				},
			},
			wantTags: []string{"backup", "critical", "production"},
		},
		{
			name:      "blobs with no tags",
			groupName: "test-group",
			blobs: map[string]*v1alpha1.Blob{
				"blob1": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "blob1",
					},
					Spec: v1alpha1.BlobSpec{
						ChunksNumber: 10,
					},
					Status: v1alpha1.BlobStatus{
						Phase: v1alpha1.BlobPhaseRunning,
					},
				},
			},
			wantTags: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregate := aggregateBlobs(tt.groupName, tt.blobs)
			if !reflect.DeepEqual(aggregate.Tags, tt.wantTags) {
				t.Errorf("aggregateBlobs() Tags = %v, want %v", aggregate.Tags, tt.wantTags)
			}
		})
	}
}
