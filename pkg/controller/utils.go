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
	"math"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
)

func decimalStringLength(n int64) int {
	if n == 0 {
		return 1
	}
	return int(math.Log10(float64(n))) + 1
}

func hexStringLength(n int64) int {
	if n == 0 {
		return 1
	}
	return int(math.Log2(float64(n))/4) + 1
}

// updateBearerStatusWithRetry is a convenience wrapper around utils.UpdateBearerStatusWithRetry
func updateBearerStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	name string,
	modifier func(*v1alpha1.Bearer) *v1alpha1.Bearer,
) (*v1alpha1.Bearer, error) {
	return utils.UpdateBearerStatusWithRetry(ctx, client, name, modifier)
}

// updateBlobStatusWithRetry is a convenience wrapper around utils.UpdateBlobStatusWithRetry
func updateBlobStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	name string,
	modifier func(*v1alpha1.Blob) *v1alpha1.Blob,
) (*v1alpha1.Blob, error) {
	return utils.UpdateBlobStatusWithRetry(ctx, client, name, modifier)
}

// updateChunkStatusWithRetry is a convenience wrapper around utils.UpdateChunkStatusWithRetry
func updateChunkStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	name string,
	modifier func(*v1alpha1.Chunk) *v1alpha1.Chunk,
) (*v1alpha1.Chunk, error) {
	return utils.UpdateChunkStatusWithRetry(ctx, client, name, modifier)
}
