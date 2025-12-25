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

package controller

import (
	"context"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/wzshiming/sss"
)

const (
	BlobNameAnnotationKey = v1alpha1.GroupName + "/blob-name"
	BlobNameLabelKey      = v1alpha1.GroupName + "/blob-name"
	BlobUIDLabelKey       = v1alpha1.GroupName + "/blob-uid"
)

type BlobController struct {
	blobHoldController      *BlobHoldController
	blobToChunkController   *BlobToChunkController
	blobFromChunkController *BlobFromChunkController
}

func NewBlobController(
	handlerName string,
	s3 map[string]*sss.SSS,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
) *BlobController {
	return &BlobController{
		blobHoldController: NewBlobHoldController(
			handlerName,
			client,
			sharedInformerFactory,
		),
		blobToChunkController: NewBlobToChunkController(
			handlerName,
			s3,
			client,
			sharedInformerFactory,
		),
		blobFromChunkController: NewBlobFromChunkController(
			handlerName,
			s3,
			client,
			sharedInformerFactory,
		),
	}
}

func (c *BlobController) Shutdown(ctx context.Context) error {
	err := c.blobHoldController.Shutdown(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (c *BlobController) Start(ctx context.Context) error {
	err := c.blobHoldController.Start(ctx)
	if err != nil {
		return err
	}

	err = c.blobToChunkController.Start(ctx)
	if err != nil {
		return err
	}

	err = c.blobFromChunkController.Start(ctx)
	if err != nil {
		return err
	}

	return nil
}
