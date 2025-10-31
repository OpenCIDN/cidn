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
// It takes a modifier function that updates the bearer status and returns the modified bearer.
// The modifier function should not mutate its input; it should return a new or copied object.
// On conflict error, this function will retry once by refetching the latest bearer and
// reapplying the modifier function.
func UpdateBearerStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	name string,
	modifier func(*v1alpha1.Bearer) *v1alpha1.Bearer,
) (*v1alpha1.Bearer, error) {
	// Get the latest version first
	latest, err := client.TaskV1alpha1().Bearers().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	bearer := modifier(latest.DeepCopy())
	updated, err := client.TaskV1alpha1().Bearers().UpdateStatus(ctx, bearer, metav1.UpdateOptions{})
	if err != nil {
		if !apierrors.IsConflict(err) {
			return nil, err
		}

		// Retry once on conflict
		latest, err = client.TaskV1alpha1().Bearers().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		bearer = modifier(latest.DeepCopy())
		updated, err = client.TaskV1alpha1().Bearers().UpdateStatus(ctx, bearer, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}

// UpdateBlobStatusWithRetry updates blob status with retry logic for conflict errors.
// It takes a modifier function that updates the blob status and returns the modified blob.
// The modifier function should not mutate its input; it should return a new or copied object.
// On conflict error, this function will retry once by refetching the latest blob and
// reapplying the modifier function.
func UpdateBlobStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	name string,
	modifier func(*v1alpha1.Blob) *v1alpha1.Blob,
) (*v1alpha1.Blob, error) {
	// Get the latest version first
	latest, err := client.TaskV1alpha1().Blobs().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	blob := modifier(latest.DeepCopy())
	updated, err := client.TaskV1alpha1().Blobs().UpdateStatus(ctx, blob, metav1.UpdateOptions{})
	if err != nil {
		if !apierrors.IsConflict(err) {
			return nil, err
		}

		// Retry once on conflict
		latest, err = client.TaskV1alpha1().Blobs().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		blob = modifier(latest.DeepCopy())
		updated, err = client.TaskV1alpha1().Blobs().UpdateStatus(ctx, blob, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}

// UpdateChunkStatusWithRetry updates chunk status with retry logic for conflict errors.
// It takes a modifier function that updates the chunk status and returns the modified chunk.
// The modifier function should not mutate its input; it should return a new or copied object.
// On conflict error, this function will retry once by refetching the latest chunk and
// reapplying the modifier function.
func UpdateChunkStatusWithRetry(
	ctx context.Context,
	client versioned.Interface,
	name string,
	modifier func(*v1alpha1.Chunk) *v1alpha1.Chunk,
) (*v1alpha1.Chunk, error) {
	// Get the latest version first
	latest, err := client.TaskV1alpha1().Chunks().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	chunk := modifier(latest.DeepCopy())
	updated, err := client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
	if err != nil {
		if !apierrors.IsConflict(err) {
			return nil, err
		}

		// Retry once on conflict
		latest, err = client.TaskV1alpha1().Chunks().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		chunk = modifier(latest.DeepCopy())
		updated, err = client.TaskV1alpha1().Chunks().UpdateStatus(ctx, chunk, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}
