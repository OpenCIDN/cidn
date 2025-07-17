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
	"fmt"
	"io"
	"net"
	"net/http"
)

// IsNetWorkError checks if the request error indicates a failure and returns an error with context
func IsNetWorkError(err error) (bool, error) {
	if err == nil {
		return false, nil
	}

	if errors.Is(err, io.EOF) {
		return true, fmt.Errorf("connection closed unexpectedly: %w", err)
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true, fmt.Errorf("unexpected EOF occurred: %w", err)
	}

	if isNetError(err) {
		return true, fmt.Errorf("network error occurred: %w", err)
	}
	return false, fmt.Errorf("unexpected error: %w", err)
}

// IsHTTPResponseError checks if the response/error indicates a failure and returns an error with context
func IsHTTPResponseError(resp *http.Response, err error) (bool, error) {
	if err != nil {
		return IsNetWorkError(err)
	}

	if resp == nil {
		return false, errors.New("nil response received")
	}

	switch {
	case resp.StatusCode >= http.StatusInternalServerError:
		return true, fmt.Errorf("server error (status %d)", resp.StatusCode)
	case resp.StatusCode == http.StatusTooManyRequests:
		return true, fmt.Errorf("rate limited (status %d)", resp.StatusCode)
	default:
		return false, nil
	}
}

// isNetError checks if the error is a network error
func isNetError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr)
}
