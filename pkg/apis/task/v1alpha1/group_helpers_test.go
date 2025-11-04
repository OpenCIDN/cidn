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
	"reflect"
	"sort"
	"testing"
)

func TestGetGroups(t *testing.T) {
	prefix := "test.io/group/"
	
	tests := []struct {
		name        string
		annotations map[string]string
		expected    []string
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			expected:    nil,
		},
		{
			name:        "empty annotations",
			annotations: map[string]string{},
			expected:    []string{},
		},
		{
			name: "single group",
			annotations: map[string]string{
				"test.io/group/group1": "",
			},
			expected: []string{"group1"},
		},
		{
			name: "multiple groups",
			annotations: map[string]string{
				"test.io/group/group1": "",
				"test.io/group/group2": "",
				"test.io/group/group3": "",
			},
			expected: []string{"group1", "group2", "group3"},
		},
		{
			name: "groups with other annotations",
			annotations: map[string]string{
				"test.io/group/group1":  "",
				"test.io/group/group2":  "",
				"test.io/other":         "value",
				"different.io/group/g3": "",
			},
			expected: []string{"group1", "group2"},
		},
		{
			name: "no matching groups",
			annotations: map[string]string{
				"test.io/other":         "value",
				"different.io/group/g3": "",
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetGroups(tt.annotations, prefix)
			// Sort both slices for comparison
			if result != nil {
				sort.Strings(result)
			}
			if tt.expected != nil {
				sort.Strings(tt.expected)
			}
			// Treat nil and empty slice as equivalent
			if len(result) == 0 && len(tt.expected) == 0 {
				return
			}
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("GetGroups() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSetGroups(t *testing.T) {
	prefix := "test.io/group/"
	
	tests := []struct {
		name        string
		initial     map[string]string
		groups      []string
		expected    map[string]string
	}{
		{
			name:     "nil annotations",
			initial:  nil,
			groups:   []string{"group1"},
			expected: nil, // SetGroups should not modify nil map
		},
		{
			name:    "set single group",
			initial: map[string]string{},
			groups:  []string{"group1"},
			expected: map[string]string{
				"test.io/group/group1": "",
			},
		},
		{
			name:    "set multiple groups",
			initial: map[string]string{},
			groups:  []string{"group1", "group2", "group3"},
			expected: map[string]string{
				"test.io/group/group1": "",
				"test.io/group/group2": "",
				"test.io/group/group3": "",
			},
		},
		{
			name: "replace existing groups",
			initial: map[string]string{
				"test.io/group/old1": "",
				"test.io/group/old2": "",
				"test.io/other":      "value",
			},
			groups: []string{"new1", "new2"},
			expected: map[string]string{
				"test.io/group/new1": "",
				"test.io/group/new2": "",
				"test.io/other":      "value",
			},
		},
		{
			name: "clear all groups",
			initial: map[string]string{
				"test.io/group/group1": "",
				"test.io/group/group2": "",
				"test.io/other":        "value",
			},
			groups: []string{},
			expected: map[string]string{
				"test.io/other": "value",
			},
		},
		{
			name: "groups with whitespace",
			initial: map[string]string{},
			groups:  []string{" group1 ", "group2", "  "},
			expected: map[string]string{
				"test.io/group/group1": "",
				"test.io/group/group2": "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			annotations := tt.initial
			SetGroups(annotations, prefix, tt.groups)
			if !reflect.DeepEqual(annotations, tt.expected) {
				t.Errorf("SetGroups() resulted in %v, want %v", annotations, tt.expected)
			}
		})
	}
}

func TestAddGroup(t *testing.T) {
	prefix := "test.io/group/"
	
	tests := []struct {
		name     string
		initial  map[string]string
		group    string
		expected map[string]string
	}{
		{
			name:     "add to empty",
			initial:  map[string]string{},
			group:    "group1",
			expected: map[string]string{"test.io/group/group1": ""},
		},
		{
			name: "add to existing",
			initial: map[string]string{
				"test.io/group/group1": "",
			},
			group: "group2",
			expected: map[string]string{
				"test.io/group/group1": "",
				"test.io/group/group2": "",
			},
		},
		{
			name:     "add with whitespace",
			initial:  map[string]string{},
			group:    "  group1  ",
			expected: map[string]string{"test.io/group/group1": ""},
		},
		{
			name:     "add empty string",
			initial:  map[string]string{},
			group:    "",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			annotations := tt.initial
			AddGroup(annotations, prefix, tt.group)
			if !reflect.DeepEqual(annotations, tt.expected) {
				t.Errorf("AddGroup() resulted in %v, want %v", annotations, tt.expected)
			}
		})
	}
}

func TestRemoveGroup(t *testing.T) {
	prefix := "test.io/group/"
	
	tests := []struct {
		name     string
		initial  map[string]string
		group    string
		expected map[string]string
	}{
		{
			name: "remove existing",
			initial: map[string]string{
				"test.io/group/group1": "",
				"test.io/group/group2": "",
			},
			group: "group1",
			expected: map[string]string{
				"test.io/group/group2": "",
			},
		},
		{
			name: "remove non-existent",
			initial: map[string]string{
				"test.io/group/group1": "",
			},
			group: "group2",
			expected: map[string]string{
				"test.io/group/group1": "",
			},
		},
		{
			name:     "remove from empty",
			initial:  map[string]string{},
			group:    "group1",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			annotations := tt.initial
			RemoveGroup(annotations, prefix, tt.group)
			if !reflect.DeepEqual(annotations, tt.expected) {
				t.Errorf("RemoveGroup() resulted in %v, want %v", annotations, tt.expected)
			}
		})
	}
}
