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
	"testing"

	"github.com/wzshiming/ioswmr"
)

func TestSWMRCountCanBeCountedWhileANewReaderStarts(t *testing.T) {
	content := testContent(70000)
	swmr := ioswmr.NewSWMR(nil)
	if _, err := swmr.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := swmr.Close(); err != nil {
		t.Fatal(err)
	}
	counter := newSWMRCount(context.Background(), swmr)
	done := make(chan error)
	go func() {
		_, err := io.Copy(io.Discard, counter.NewReader())
		done <- err
	}()
	// The progress updater counts while an upload starts its reader.
	counter.Count()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := counter.Count(); got != int64(len(content)) {
		t.Errorf("got count %d after the reader reached the end, want %d", got, len(content))
	}
}
