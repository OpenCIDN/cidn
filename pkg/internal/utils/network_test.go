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
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestIsNetWorkError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantIsNet bool
	}{
		{
			name:      "nil error",
			err:       nil,
			wantIsNet: false,
		},
		{
			name:      "io.EOF",
			err:       io.EOF,
			wantIsNet: true,
		},
		{
			name:      "io.ErrUnexpectedEOF",
			err:       io.ErrUnexpectedEOF,
			wantIsNet: true,
		},
		{
			name:      "network timeout error",
			err:       &net.DNSError{IsTimeout: true},
			wantIsNet: true,
		},
		{
			name:      "network temporary error",
			err:       &net.DNSError{IsTemporary: true},
			wantIsNet: true,
		},
		{
			name:      "generic error",
			err:       errors.New("some error"),
			wantIsNet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsNet, gotErr := IsNetWorkError(tt.err)
			if gotIsNet != tt.wantIsNet {
				t.Errorf("IsNetWorkError() isNet = %v, want %v", gotIsNet, tt.wantIsNet)
			}
			if tt.err == nil {
				if gotErr != nil {
					t.Errorf("IsNetWorkError() error = %v, want nil", gotErr)
				}
			} else {
				if gotErr == nil {
					t.Errorf("IsNetWorkError() error = nil, want non-nil")
				}
			}
		})
	}
}

func TestIsHTTPResponseError(t *testing.T) {
	tests := []struct {
		name       string
		resp       *http.Response
		err        error
		wantIsErr  bool
		wantErrNil bool
	}{
		{
			name: "successful response",
			resp: &http.Response{
				StatusCode: http.StatusOK,
			},
			err:        nil,
			wantIsErr:  false,
			wantErrNil: true,
		},
		{
			name: "server error",
			resp: &http.Response{
				StatusCode: http.StatusInternalServerError,
			},
			err:        nil,
			wantIsErr:  true,
			wantErrNil: false,
		},
		{
			name: "bad gateway",
			resp: &http.Response{
				StatusCode: http.StatusBadGateway,
			},
			err:        nil,
			wantIsErr:  true,
			wantErrNil: false,
		},
		{
			name: "service unavailable",
			resp: &http.Response{
				StatusCode: http.StatusServiceUnavailable,
			},
			err:        nil,
			wantIsErr:  true,
			wantErrNil: false,
		},
		{
			name: "too many requests",
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
			},
			err:        nil,
			wantIsErr:  true,
			wantErrNil: false,
		},
		{
			name: "client error - not retryable",
			resp: &http.Response{
				StatusCode: http.StatusBadRequest,
			},
			err:        nil,
			wantIsErr:  false,
			wantErrNil: true,
		},
		{
			name: "not found - not retryable",
			resp: &http.Response{
				StatusCode: http.StatusNotFound,
			},
			err:        nil,
			wantIsErr:  false,
			wantErrNil: true,
		},
		{
			name:       "network error with nil response",
			resp:       nil,
			err:        io.EOF,
			wantIsErr:  true,
			wantErrNil: false,
		},
		{
			name:       "nil response and nil error",
			resp:       nil,
			err:        nil,
			wantIsErr:  false,
			wantErrNil: false,
		},
		{
			name:       "generic error",
			resp:       nil,
			err:        errors.New("some error"),
			wantIsErr:  false,
			wantErrNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsErr, gotErr := IsHTTPResponseError(tt.resp, tt.err)
			if gotIsErr != tt.wantIsErr {
				t.Errorf("IsHTTPResponseError() isErr = %v, want %v", gotIsErr, tt.wantIsErr)
			}
			if tt.wantErrNil {
				if gotErr != nil {
					t.Errorf("IsHTTPResponseError() error = %v, want nil", gotErr)
				}
			} else {
				if gotErr == nil {
					t.Errorf("IsHTTPResponseError() error = nil, want non-nil")
				}
			}
		})
	}
}

// mockNetError implements net.Error interface for testing
type mockNetError struct {
	timeout   bool
	temporary bool
	msg       string
}

func (m *mockNetError) Error() string {
	return m.msg
}

func (m *mockNetError) Timeout() bool {
	return m.timeout
}

func (m *mockNetError) Temporary() bool {
	return m.temporary
}

func TestIsNetWorkError_WithMockNetError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantIsNet bool
	}{
		{
			name: "timeout error",
			err: &mockNetError{
				timeout: true,
				msg:     "timeout",
			},
			wantIsNet: true,
		},
		{
			name: "temporary error",
			err: &mockNetError{
				temporary: true,
				msg:       "temporary",
			},
			wantIsNet: true,
		},
		{
			name: "wrapped EOF",
			err: &wrappedError{
				msg: "connection failed",
				err: io.EOF,
			},
			wantIsNet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsNet, gotErr := IsNetWorkError(tt.err)
			if gotIsNet != tt.wantIsNet {
				t.Errorf("IsNetWorkError() isNet = %v, want %v", gotIsNet, tt.wantIsNet)
			}
			if gotErr == nil {
				t.Errorf("IsNetWorkError() error = nil, want non-nil")
			}
		})
	}
}

// wrappedError wraps an error for testing
type wrappedError struct {
	msg string
	err error
}

func (w *wrappedError) Error() string {
	return w.msg
}

func (w *wrappedError) Unwrap() error {
	return w.err
}

// Test real network timeout scenario
func TestIsNetWorkError_RealNetworkTimeout(t *testing.T) {
	// Create a real network timeout error
	_, err := net.DialTimeout("tcp", "192.0.2.1:80", 1*time.Nanosecond)
	if err == nil {
		t.Skip("Expected timeout error, got nil")
	}

	isNet, gotErr := IsNetWorkError(err)
	if !isNet {
		t.Errorf("IsNetWorkError() isNet = false, want true for real network timeout")
	}
	if gotErr == nil {
		t.Errorf("IsNetWorkError() error = nil, want non-nil")
	}
}
