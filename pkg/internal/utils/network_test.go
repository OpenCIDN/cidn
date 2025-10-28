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

type mockNetError struct {
	error
	temporary bool
	timeout   bool
}

func (e *mockNetError) Temporary() bool { return e.temporary }
func (e *mockNetError) Timeout() bool   { return e.timeout }

func TestIsNetWorkError(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantIsNetwork bool
	}{
		{
			name:          "nil error",
			err:           nil,
			wantIsNetwork: false,
		},
		{
			name:          "EOF error",
			err:           io.EOF,
			wantIsNetwork: true,
		},
		{
			name:          "unexpected EOF error",
			err:           io.ErrUnexpectedEOF,
			wantIsNetwork: true,
		},
		{
			name:          "network timeout error",
			err:           &mockNetError{error: errors.New("timeout"), timeout: true},
			wantIsNetwork: true,
		},
		{
			name:          "network temporary error",
			err:           &mockNetError{error: errors.New("temporary"), temporary: true},
			wantIsNetwork: true,
		},
		{
			name:          "generic error",
			err:           errors.New("some error"),
			wantIsNetwork: false,
		},
		{
			name:          "wrapped EOF",
			err:           errors.Join(io.EOF, errors.New("connection failed")),
			wantIsNetwork: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsNetwork, gotErr := IsNetWorkError(tt.err)

			if gotIsNetwork != tt.wantIsNetwork {
				t.Errorf("IsNetWorkError() isNetwork = %v, want %v", gotIsNetwork, tt.wantIsNetwork)
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
		name          string
		resp          *http.Response
		err           error
		wantIsRetry   bool
		wantErrNotNil bool
	}{
		{
			name:          "nil response and nil error",
			resp:          nil,
			err:           nil,
			wantIsRetry:   false,
			wantErrNotNil: true,
		},
		{
			name:          "nil response with error",
			resp:          nil,
			err:           errors.New("request failed"),
			wantIsRetry:   false,
			wantErrNotNil: true,
		},
		{
			name:          "network error",
			resp:          nil,
			err:           io.EOF,
			wantIsRetry:   true,
			wantErrNotNil: true,
		},
		{
			name:          "200 OK response",
			resp:          &http.Response{StatusCode: http.StatusOK},
			err:           nil,
			wantIsRetry:   false,
			wantErrNotNil: false,
		},
		{
			name:          "404 Not Found",
			resp:          &http.Response{StatusCode: http.StatusNotFound},
			err:           nil,
			wantIsRetry:   false,
			wantErrNotNil: false,
		},
		{
			name:          "429 Too Many Requests",
			resp:          &http.Response{StatusCode: http.StatusTooManyRequests},
			err:           nil,
			wantIsRetry:   true,
			wantErrNotNil: true,
		},
		{
			name:          "500 Internal Server Error",
			resp:          &http.Response{StatusCode: http.StatusInternalServerError},
			err:           nil,
			wantIsRetry:   true,
			wantErrNotNil: true,
		},
		{
			name:          "502 Bad Gateway",
			resp:          &http.Response{StatusCode: http.StatusBadGateway},
			err:           nil,
			wantIsRetry:   true,
			wantErrNotNil: true,
		},
		{
			name:          "503 Service Unavailable",
			resp:          &http.Response{StatusCode: http.StatusServiceUnavailable},
			err:           nil,
			wantIsRetry:   true,
			wantErrNotNil: true,
		},
		{
			name:          "400 Bad Request",
			resp:          &http.Response{StatusCode: http.StatusBadRequest},
			err:           nil,
			wantIsRetry:   false,
			wantErrNotNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsRetry, gotErr := IsHTTPResponseError(tt.resp, tt.err)

			if gotIsRetry != tt.wantIsRetry {
				t.Errorf("IsHTTPResponseError() isRetry = %v, want %v", gotIsRetry, tt.wantIsRetry)
			}

			if tt.wantErrNotNil && gotErr == nil {
				t.Errorf("IsHTTPResponseError() error = nil, want non-nil")
			} else if !tt.wantErrNotNil && gotErr != nil {
				t.Errorf("IsHTTPResponseError() error = %v, want nil", gotErr)
			}
		})
	}
}

func TestIsNetError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "network timeout error",
			err:  &mockNetError{error: errors.New("timeout"), timeout: true},
			want: true,
		},
		{
			name: "network temporary error",
			err:  &mockNetError{error: errors.New("temporary"), temporary: true},
			want: true,
		},
		{
			name: "net.OpError",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			want: true,
		},
		{
			name: "generic error",
			err:  errors.New("not a network error"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNetError(tt.err)
			if got != tt.want {
				t.Errorf("isNetError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNetWorkErrorWithRealNetErrors(t *testing.T) {
	// Test with a real network timeout error
	conn, err := net.DialTimeout("tcp", "192.0.2.1:80", 1*time.Nanosecond)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		isNet, wrappedErr := IsNetWorkError(err)
		if !isNet {
			t.Errorf("IsNetWorkError() with real timeout should be network error")
		}
		if wrappedErr == nil {
			t.Errorf("IsNetWorkError() should return wrapped error")
		}
	}
}
