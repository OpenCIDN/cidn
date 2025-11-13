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

	"github.com/wzshiming/ioswmr"
)

// readCount tracks the number of bytes read through an io.Reader
type readCount struct {
	ctx    context.Context
	reader io.Reader
	count  int64
	mut    sync.RWMutex
}

// newReadCount returns a new readCount that wraps the given reader
func newReadCount(ctx context.Context, r io.Reader) *readCount {
	return &readCount{ctx: ctx, reader: r}
}

// Read implements io.Reader and tracks bytes read
func (r *readCount) Read(p []byte) (int, error) {
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
func (r *readCount) Count() int64 {
	r.mut.RLock()
	defer r.mut.RUnlock()
	return r.count
}

// swmrCount wraps an ioswmr.SWMR and tracks the number of bytes read
type swmrCount struct {
	swmr ioswmr.SWMR
	ctx  context.Context
	rc   *readCount
	mut  sync.RWMutex
}

// newReadCount returns a new ReadCount that wraps the given reader
func newSWMRCount(ctx context.Context, swmr ioswmr.SWMR) *swmrCount {
	return &swmrCount{ctx: ctx, swmr: swmr}
}

// Count returns the total number of bytes read
func (r *swmrCount) Count() int64 {
	r.mut.RLock()
	defer r.mut.RUnlock()
	if r.rc == nil {
		return 0
	}
	return r.rc.Count()
}

func (r *swmrCount) NewReader() io.Reader {
	r.mut.RLock()
	defer r.mut.RUnlock()
	r.rc = newReadCount(r.ctx, r.swmr.NewReader())
	return r.rc
}
