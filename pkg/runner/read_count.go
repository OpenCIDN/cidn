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
	"context"
	"io"
	"sync"
)

// ReadCount tracks the number of bytes read through an io.Reader
type ReadCount struct {
	ctx    context.Context
	reader io.Reader
	count  int64
	mut    sync.RWMutex
}

// NewReadCount returns a new ReadCount that wraps the given reader
func NewReadCount(ctx context.Context, r io.Reader) *ReadCount {
	return &ReadCount{ctx: ctx, reader: r}
}

// Read implements io.Reader and tracks bytes read
func (r *ReadCount) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)

	r.mut.Lock()
	r.count += int64(n)
	r.mut.Unlock()

	return n, err
}

// Count returns the total number of bytes read
func (r *ReadCount) Count() int64 {
	r.mut.RLock()
	defer r.mut.RUnlock()
	return r.count
}
