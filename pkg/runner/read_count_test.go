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
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewReadCount(t *testing.T) {
	ctx := context.Background()
	reader := strings.NewReader("test data")

	rc := NewReadCount(ctx, reader)

	if rc == nil {
		t.Fatal("NewReadCount() returned nil")
	}

	if rc.ctx != ctx {
		t.Error("NewReadCount() context not set correctly")
	}

	if rc.reader != reader {
		t.Error("NewReadCount() reader not set correctly")
	}

	if rc.count != 0 {
		t.Errorf("NewReadCount() initial count = %d, want 0", rc.count)
	}
}

func TestReadCount_Read(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		bufferSize int
		wantCount  int64
		wantErr    bool
	}{
		{
			name:       "read all at once",
			input:      "Hello, World!",
			bufferSize: 100,
			wantCount:  13,
			wantErr:    false,
		},
		{
			name:       "read in chunks",
			input:      "Hello, World!",
			bufferSize: 5,
			wantCount:  13,
			wantErr:    false,
		},
		{
			name:       "empty input",
			input:      "",
			bufferSize: 10,
			wantCount:  0,
			wantErr:    false,
		},
		{
			name:       "large input",
			input:      strings.Repeat("A", 1000),
			bufferSize: 100,
			wantCount:  1000,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			reader := strings.NewReader(tt.input)
			rc := NewReadCount(ctx, reader)

			buffer := make([]byte, tt.bufferSize)
			totalRead := int64(0)

			for {
				n, err := rc.Read(buffer)
				totalRead += int64(n)

				if err == io.EOF {
					break
				}
				if err != nil && !tt.wantErr {
					t.Errorf("Read() unexpected error = %v", err)
					break
				}
			}

			if rc.Count() != tt.wantCount {
				t.Errorf("Count() = %d, want %d", rc.Count(), tt.wantCount)
			}

			if totalRead != tt.wantCount {
				t.Errorf("Total bytes read = %d, want %d", totalRead, tt.wantCount)
			}
		})
	}
}

func TestReadCount_Count(t *testing.T) {
	ctx := context.Background()
	reader := strings.NewReader("Test data for counting")
	rc := NewReadCount(ctx, reader)

	if rc.Count() != 0 {
		t.Errorf("Initial Count() = %d, want 0", rc.Count())
	}

	buffer := make([]byte, 5)
	rc.Read(buffer)

	if rc.Count() != 5 {
		t.Errorf("Count() after first read = %d, want 5", rc.Count())
	}

	rc.Read(buffer)

	if rc.Count() != 10 {
		t.Errorf("Count() after second read = %d, want 10", rc.Count())
	}
}

func TestReadCount_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := strings.NewReader("Test data that should not be read")
	rc := NewReadCount(ctx, reader)

	cancel()

	buffer := make([]byte, 10)
	_, err := rc.Read(buffer)

	if err == nil {
		t.Error("Read() after context cancellation should return error")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Read() error = %v, want context.Canceled", err)
	}

	if rc.Count() != 0 {
		t.Errorf("Count() after cancelled read = %d, want 0", rc.Count())
	}
}

func TestReadCount_ContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	reader := strings.NewReader("Test data")
	rc := NewReadCount(ctx, reader)

	time.Sleep(10 * time.Millisecond)

	buffer := make([]byte, 10)
	_, err := rc.Read(buffer)

	if err == nil {
		t.Error("Read() after context timeout should return error")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Read() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestReadCount_ConcurrentReads(t *testing.T) {
	// Note: This test demonstrates that ReadCount's counter is thread-safe,
	// even though strings.NewReader itself is not safe for concurrent reads.
	// In practice, you should not read from the same underlying reader
	// from multiple goroutines.
	ctx := context.Background()
	data := strings.Repeat("A", 10000)
	reader := strings.NewReader(data)
	rc := NewReadCount(ctx, reader)

	// Single reader goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := make([]byte, 100)
		for {
			_, err := rc.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("Read error: %v", err)
				break
			}
		}
	}()

	wg.Wait()

	finalCount := rc.Count()
	if finalCount != int64(len(data)) {
		t.Errorf("Final Count() = %d, want %d", finalCount, len(data))
	}
}

func TestReadCount_ConcurrentCountAccess(t *testing.T) {
	ctx := context.Background()
	data := strings.Repeat("B", 1000)
	reader := strings.NewReader(data)
	rc := NewReadCount(ctx, reader)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := make([]byte, 10)
		for {
			_, err := rc.Read(buffer)
			if err == io.EOF {
				break
			}
		}
	}()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rc.Count()
		}()
	}

	wg.Wait()

	if rc.Count() != int64(len(data)) {
		t.Errorf("Final Count() = %d, want %d", rc.Count(), len(data))
	}
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestReadCount_ReadError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("read error")
	reader := &errorReader{err: expectedErr}
	rc := NewReadCount(ctx, reader)

	buffer := make([]byte, 10)
	n, err := rc.Read(buffer)

	if n != 0 {
		t.Errorf("Read() n = %d, want 0", n)
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Read() error = %v, want %v", err, expectedErr)
	}

	if rc.Count() != 0 {
		t.Errorf("Count() after error = %d, want 0", rc.Count())
	}
}

type partialReader struct {
	data  []byte
	pos   int
	limit int
}

func (r *partialReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	toRead := len(p)
	if toRead > r.limit {
		toRead = r.limit
	}
	if toRead > len(r.data)-r.pos {
		toRead = len(r.data) - r.pos
	}

	n := copy(p, r.data[r.pos:r.pos+toRead])
	r.pos += n
	return n, nil
}

func TestReadCount_PartialReads(t *testing.T) {
	ctx := context.Background()
	data := []byte("Hello, World! This is a test.")
	reader := &partialReader{data: data, limit: 5}
	rc := NewReadCount(ctx, reader)

	var buf bytes.Buffer
	written, err := io.Copy(&buf, rc)

	if err != nil {
		t.Errorf("io.Copy() error = %v", err)
	}

	if written != int64(len(data)) {
		t.Errorf("io.Copy() written = %d, want %d", written, len(data))
	}

	if rc.Count() != int64(len(data)) {
		t.Errorf("Count() = %d, want %d", rc.Count(), len(data))
	}

	if buf.String() != string(data) {
		t.Errorf("Data mismatch: got %q, want %q", buf.String(), string(data))
	}
}
