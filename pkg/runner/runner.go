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

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
)

// SyncRunner executes Sync tasks
type Runner struct {
	sync *SyncRunner
	head *HeadRunner

	sharedInformerFactory externalversions.SharedInformerFactory
}

// NewSyncRunner creates a new Runner instance
func NewRunner(handlerName string, client versioned.Interface) *Runner {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)
	return &Runner{
		sync:                  NewSyncRunner(handlerName, client, sharedInformerFactory),
		head:                  NewHeadRunner(handlerName, client, sharedInformerFactory),
		sharedInformerFactory: sharedInformerFactory,
	}
}

// Start starts the runner
func (r *Runner) Start(ctx context.Context) error {
	err := r.sync.Start(ctx)
	if err != nil {
		return err
	}
	err = r.head.Start(ctx)
	if err != nil {
		return err
	}

	r.sharedInformerFactory.Start(ctx.Done())
	return nil
}
