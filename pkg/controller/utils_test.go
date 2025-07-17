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

package controller

import (
	"testing"
)

func TestDecimalStringLength(t *testing.T) {
	tests := []struct {
		name string
		n    int64
		want int
	}{
		{"zero", 0, 1},
		{"single digit", 9, 1},
		{"two digits", 10, 2},
		{"three digits", 100, 3},
		{"large number", 1234567890, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decimalStringLength(tt.n); got != tt.want {
				t.Errorf("decimalStringLength() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHexStringLength(t *testing.T) {
	tests := []struct {
		name string
		n    int64
		want int
	}{
		{"zero", 0, 1},
		{"single hex digit", 0xF, 1},
		{"two hex digits", 0x10, 2},
		{"three hex digits", 0x100, 3},
		{"large hex number", 0x123456789ABCDEF, 15},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hexStringLength(tt.n); got != tt.want {
				t.Errorf("hexStringLength() = %v, want %v", got, tt.want)
			}
		})
	}
}
