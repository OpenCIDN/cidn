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

package runner

import (
	"errors"
	"testing"
)

func TestErrBearerExpired(t *testing.T) {
	msg := "bearer token has expired, waiting for next refresh"
	err := &ErrBearerExpired{msg: msg}
	
	if err.Error() != msg {
		t.Errorf("Expected error message %q, got %q", msg, err.Error())
	}
	
	// Test type assertion
	var testErr error = err
	if _, ok := testErr.(*ErrBearerExpired); !ok {
		t.Error("Type assertion to *ErrBearerExpired should succeed")
	}
	
	// Test errors.As
	var bearerErr *ErrBearerExpired
	if !errors.As(testErr, &bearerErr) {
		t.Error("errors.As should identify ErrBearerExpired")
	}
	if bearerErr.Error() != msg {
		t.Errorf("Expected error message %q from errors.As, got %q", msg, bearerErr.Error())
	}
}
