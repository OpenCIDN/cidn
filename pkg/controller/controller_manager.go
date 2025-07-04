package controller

import (
	"context"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/wzshiming/sss"
)

type ControllerManager struct {
	blobController *BlobController
}

func NewControllerManager(handlerName string, client *versioned.Clientset, s3 *sss.SSS) (*ControllerManager, error) {
	blobController := NewBlobController(
		handlerName,
		s3,
		client,
	)

	return &ControllerManager{
		blobController: blobController,
	}, nil
}

func (m *ControllerManager) Start(ctx context.Context) error {
	err := m.blobController.Start(ctx)
	if err != nil {
		return err
	}

	return nil
}
