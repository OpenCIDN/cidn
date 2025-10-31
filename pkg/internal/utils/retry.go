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

package utils

import (
	"context"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UpdateBearerStatusWithRetry updates bearer status with retry logic for conflict errors.
// It takes a bearer object and a modifier function that updates the bearer status.
// The modifier function should not mutate its input; it should return a new or copied object.
// On conflict error, this function will retry once by refetching the latest bearer and
// reapplying the modifier function.
func UpdateBearerStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	bearer *v1alpha1.Bearer,
	modifier func(*v1alpha1.Bearer) *v1alpha1.Bearer,
) (*v1alpha1.Bearer, error) {
	// Try to update with the passed-in object first
	modified := modifier(bearer.DeepCopy())
	updated, err := client.TaskV1alpha1().Bearers().UpdateStatus(ctx, modified, metav1.UpdateOptions{})
	if err != nil {
		if !apierrors.IsConflict(err) {
			return nil, err
		}

		// Retry once on conflict by fetching the latest version
		latest, err := client.TaskV1alpha1().Bearers().Get(ctx, bearer.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		modified = modifier(latest.DeepCopy())
		updated, err = client.TaskV1alpha1().Bearers().UpdateStatus(ctx, modified, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}

// UpdateBlobStatusWithRetry updates blob status with retry logic for conflict errors.
// It takes a blob object and a modifier function that updates the blob status.
// The modifier function should not mutate its input; it should return a new or copied object.
// On conflict error, this function will retry once by refetching the latest blob and
// reapplying the modifier function.
func UpdateBlobStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	blob *v1alpha1.Blob,
	modifier func(*v1alpha1.Blob) *v1alpha1.Blob,
) (*v1alpha1.Blob, error) {
	// Try to update with the passed-in object first
	modified := modifier(blob.DeepCopy())
	updated, err := client.TaskV1alpha1().Blobs().UpdateStatus(ctx, modified, metav1.UpdateOptions{})
	if err != nil {
		if !apierrors.IsConflict(err) {
			return nil, err
		}

		// Retry once on conflict by fetching the latest version
		latest, err := client.TaskV1alpha1().Blobs().Get(ctx, blob.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		modified = modifier(latest.DeepCopy())
		updated, err = client.TaskV1alpha1().Blobs().UpdateStatus(ctx, modified, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}

// UpdateChunkStatusWithRetry updates chunk status with retry logic for conflict errors.
// It takes a chunk object and a modifier function that updates the chunk status.
// The modifier function should not mutate its input; it should return a new or copied object.
// On conflict error, this function will retry once by refetching the latest chunk and
// reapplying the modifier function.
func UpdateChunkStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	chunk *v1alpha1.Chunk,
	modifier func(*v1alpha1.Chunk) *v1alpha1.Chunk,
) (*v1alpha1.Chunk, error) {
	// Try to update with the passed-in object first
	modified := modifier(chunk.DeepCopy())
	updated, err := client.TaskV1alpha1().Chunks().UpdateStatus(ctx, modified, metav1.UpdateOptions{})
	if err != nil {
		if !apierrors.IsConflict(err) {
			return nil, err
		}

		// Retry once on conflict by fetching the latest version
		latest, err := client.TaskV1alpha1().Chunks().Get(ctx, chunk.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		modified = modifier(latest.DeepCopy())
		updated, err = client.TaskV1alpha1().Chunks().UpdateStatus(ctx, modified, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}
