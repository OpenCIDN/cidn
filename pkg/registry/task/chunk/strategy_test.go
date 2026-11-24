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

package chunk

import (
	"strings"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

func create(destinations int, firstPart *v1alpha1.ChunkFirstPart) error {
	scheme := runtime.NewScheme()
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	chunk := &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{Name: "chunk"},
		Spec: v1alpha1.ChunkSpec{
			Source:      v1alpha1.ChunkHTTP{Request: v1alpha1.ChunkHTTPRequest{Method: "GET", URL: "http://example.com/blob"}},
			Destination: make([]v1alpha1.ChunkHTTP, destinations),
			FirstPart:   firstPart,
		},
	}
	rest.FillObjectMetaSystemFields(chunk)
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceNone)
	return rest.BeforeCreate(NewStrategy(scheme), ctx, chunk)
}

func TestCreatingAFirstPartWithoutOneSizePerDestinationIsRejected(t *testing.T) {
	tests := []struct {
		name          string
		destinations  int
		existingSizes []int64
	}{
		{name: "fewer sizes than destinations", destinations: 2, existingSizes: []int64{5}},
		{name: "more sizes than destinations", destinations: 1, existingSizes: []int64{5, 0}},
		{name: "sizes without destinations", destinations: 0, existingSizes: []int64{5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := create(tt.destinations, &v1alpha1.ChunkFirstPart{ExistingSizes: tt.existingSizes})
			if err == nil || !strings.Contains(err.Error(), "spec.firstPart.existingSizes") {
				t.Errorf("create error = %v, want a field error on spec.firstPart.existingSizes", err)
			}
		})
	}
}

func TestCreatingAChunkWithOneSizePerDestinationOrNoneIsAccepted(t *testing.T) {
	tests := []struct {
		name      string
		firstPart *v1alpha1.ChunkFirstPart
	}{
		{name: "one size per destination", firstPart: &v1alpha1.ChunkFirstPart{ExistingSizes: []int64{5, 0}}},
		{name: "sizes omitted", firstPart: &v1alpha1.ChunkFirstPart{}},
		{name: "no first part"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := create(2, tt.firstPart); err != nil {
				t.Errorf("create rejected: %v", err)
			}
		})
	}
}
