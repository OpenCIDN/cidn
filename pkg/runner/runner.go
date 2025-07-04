package runner

import (
	"context"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
)

// SyncRunner executes Sync tasks
type Runner struct {
	sync *SyncRunner
	head *HeadRunner
}

// NewSyncRunner creates a new Runner instance
func NewRunner(handlerName string, clientset versioned.Interface) *Runner {
	return &Runner{
		sync: NewSyncRunner(handlerName, clientset),
		head: NewHeadRunner(handlerName, clientset),
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

	return nil
}
