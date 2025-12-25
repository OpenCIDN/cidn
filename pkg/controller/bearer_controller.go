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
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
)

const (
	BearerNameAnnotationKey = v1alpha1.GroupName + "/bearer-name"
	BearerNameLabelKey      = v1alpha1.GroupName + "/bearer-name"
	BearerUIDLabelKey       = v1alpha1.GroupName + "/bearer-uid"
)

type BearerController struct {
	bearerHoldController      *BearerHoldController
	bearerToChunkController   *BearerToChunkController
	bearerFromChunkController *BearerFromChunkController
	chunkToBearerController   *ChunkToBearerController
	chunkFromBearerController *ChunkFromBearerController
}

func NewBearerController(
	handlerName string,
	client versioned.Interface,
	sharedInformerFactory externalversions.SharedInformerFactory,
	users []utils.UserValue,
) *BearerController {
	return &BearerController{
		bearerHoldController: NewBearerHoldController(
			handlerName,
			client,
			sharedInformerFactory,
		),
		bearerToChunkController: NewBearerToChunkController(
			handlerName,
			client,
			sharedInformerFactory,
			users,
		),
		bearerFromChunkController: NewBearerFromChunkController(
			handlerName,
			client,
			sharedInformerFactory,
		),
		chunkToBearerController: NewChunkToBearerController(
			handlerName,
			client,
			sharedInformerFactory,
		),
		chunkFromBearerController: NewChunkFromBearerController(
			handlerName,
			client,
			sharedInformerFactory,
		),
	}
}

func (c *BearerController) Shutdown(ctx context.Context) error {
	err := c.bearerHoldController.Shutdown(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (c *BearerController) Start(ctx context.Context) error {
	err := c.bearerHoldController.Start(ctx)
	if err != nil {
		return err
	}

	err = c.bearerToChunkController.Start(ctx)
	if err != nil {
		return err
	}

	err = c.bearerFromChunkController.Start(ctx)
	if err != nil {
		return err
	}

	err = c.chunkToBearerController.Start(ctx)
	if err != nil {
		return err
	}

	err = c.chunkFromBearerController.Start(ctx)
	if err != nil {
		return err
	}

	return nil
}
