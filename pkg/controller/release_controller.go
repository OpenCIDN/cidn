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

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
)

type ReleaseController struct {
	releaseBlobController   *ReleaseBlobController
	releaseChunkController  *ReleaseChunkController
	releaseBearerController *ReleaseBearerController
}

func NewReleaseController(handlerName string, client *versioned.Clientset, sharedInformerFactory externalversions.SharedInformerFactory) *ReleaseController {
	releaseBlobController := NewReleaseBlobController(
		handlerName,
		client,
		sharedInformerFactory,
	)

	releaseChunkController := NewReleaseChunkController(
		handlerName,
		client,
		sharedInformerFactory,
	)

	releaseBearerController := NewReleaseBearerController(
		handlerName,
		client,
		sharedInformerFactory,
	)

	return &ReleaseController{
		releaseBlobController:   releaseBlobController,
		releaseChunkController:  releaseChunkController,
		releaseBearerController: releaseBearerController,
	}
}

func (c *ReleaseController) Start(ctx context.Context) error {
	err := c.releaseBlobController.Start(ctx)
	if err != nil {
		return err
	}
	err = c.releaseChunkController.Start(ctx)
	if err != nil {
		return err
	}
	err = c.releaseBearerController.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}
