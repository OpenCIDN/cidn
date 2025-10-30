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

package utils

import (
	"strings"
	"testing"
)

func TestIdentity(t *testing.T) {
	// Test that Identity() returns a non-empty string
	id, err := Identity()
	if err != nil {
		t.Fatalf("Identity() error = %v, want nil", err)
	}
	if id == "" {
		t.Error("Identity() returned empty string")
	}

	// Test format: should be "hexstring-timestamp"
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		t.Errorf("Identity() format = %v, want 'hex-timestamp' with 2 parts", id)
	}

	// Test that hex part is 16 characters
	hexPart := parts[0]
	if len(hexPart) != 16 {
		t.Errorf("Identity() hex part length = %d, want 16", len(hexPart))
	}

	// Test that hex part is valid hex
	for _, c := range hexPart {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("Identity() hex part contains invalid character: %c", c)
		}
	}

	// Test that timestamp part is numeric
	timestampPart := parts[1]
	for _, c := range timestampPart {
		if c < '0' || c > '9' {
			t.Errorf("Identity() timestamp part contains non-digit: %c", c)
		}
	}
}

func TestIdentity_Multiple(t *testing.T) {
	// Test that calling Identity() multiple times returns different values
	// (due to different timestamps)
	id1, err := Identity()
	if err != nil {
		t.Fatalf("Identity() first call error = %v, want nil", err)
	}

	id2, err := Identity()
	if err != nil {
		t.Fatalf("Identity() second call error = %v, want nil", err)
	}

	// The IDs should be different because timestamps are different
	// Note: In very rare cases on very fast systems, timestamps might be the same
	// but that's acceptable for this test
	if id1 == id2 {
		t.Logf("Identity() returned same value twice: %v (acceptable if timestamps are equal)", id1)
	}

	// The hex parts should be the same (same hostname)
	parts1 := strings.Split(id1, "-")
	parts2 := strings.Split(id2, "-")
	if parts1[0] != parts2[0] {
		t.Errorf("Identity() hex parts differ: %v vs %v (should be same for same hostname)", parts1[0], parts2[0])
	}
}

func TestIdentity_Format(t *testing.T) {
	id, err := Identity()
	if err != nil {
		t.Fatalf("Identity() error = %v, want nil", err)
	}

	// Verify the identity matches expected pattern
	if !strings.Contains(id, "-") {
		t.Errorf("Identity() = %v, want to contain '-'", id)
	}

	// Verify overall length is reasonable
	// 16 hex chars + 1 dash + at least 10 digit timestamp = at least 27 chars
	if len(id) < 27 {
		t.Errorf("Identity() length = %d, want at least 27", len(id))
	}
}
