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
	"testing"
)

func TestParseGroups(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single group",
			input:    "group1",
			expected: []string{"group1"},
		},
		{
			name:     "multiple groups",
			input:    "group1,group2,group3",
			expected: []string{"group1", "group2", "group3"},
		},
		{
			name:     "groups with spaces",
			input:    "group1, group2 , group3",
			expected: []string{"group1", "group2", "group3"},
		},
		{
			name:     "groups with empty values",
			input:    "group1,,group2,",
			expected: []string{"group1", "group2"},
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseGroups(tt.input)
			// Treat nil and empty slice as equivalent
			if len(result) == 0 && len(tt.expected) == 0 {
				return
			}
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("ParseGroups(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatGroups(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "nil slice",
			input:    nil,
			expected: "",
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: "",
		},
		{
			name:     "single group",
			input:    []string{"group1"},
			expected: "group1",
		},
		{
			name:     "multiple groups",
			input:    []string{"group1", "group2", "group3"},
			expected: "group1,group2,group3",
		},
		{
			name:     "groups with spaces",
			input:    []string{" group1 ", "group2", " group3"},
			expected: "group1,group2,group3",
		},
		{
			name:     "groups with empty values",
			input:    []string{"group1", "", "group2"},
			expected: "group1,group2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatGroups(tt.input)
			if result != tt.expected {
				t.Errorf("FormatGroups(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseFormatRoundTrip(t *testing.T) {
	tests := []string{
		"group1",
		"group1,group2",
		"group1,group2,group3",
		"alpha,beta,gamma,delta",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			parsed := ParseGroups(input)
			formatted := FormatGroups(parsed)
			if formatted != input {
				t.Errorf("Round trip failed: input %q, got %q", input, formatted)
			}
		})
	}
}
