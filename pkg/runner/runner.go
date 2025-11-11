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
	"time"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
)

// ChunkRunner executes Chunk tasks
type Runner struct {
	chunk *ChunkRunner

	sharedInformerFactory externalversions.SharedInformerFactory
}

// NewChunkRunner creates a new Runner instance
func NewRunner(handlerName string, client versioned.Interface, updateDuration time.Duration) *Runner {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)
	return &Runner{
		chunk:                 NewChunkRunner(handlerName, client, sharedInformerFactory, updateDuration),
		sharedInformerFactory: sharedInformerFactory,
	}
}

// Shutdown stops the runner
func (r *Runner) Shutdown(ctx context.Context) error {
	err := r.chunk.Release(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Start starts the runner
func (r *Runner) Start(ctx context.Context) error {
	err := r.chunk.Start(ctx)
	if err != nil {
		return err
	}

	r.sharedInformerFactory.Start(make(<-chan struct{}))
	return nil
}
