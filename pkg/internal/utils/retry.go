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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type object[T any] interface {
	GetName() string
	DeepCopy() T
}

type client[T any] interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (T, error)
	UpdateStatus(ctx context.Context, resource T, opts metav1.UpdateOptions) (T, error)
}

func UpdateResourceStatusWithRetry[T object[T]](
	ctx context.Context,
	client client[T],
	latest T,
	modifier func(T) T,
) (T, error) {
	var t T
	name := latest.GetName()
	latest = latest.DeepCopy()

	resource := modifier(latest)
	updated, err := client.UpdateStatus(ctx, resource, metav1.UpdateOptions{})
	if err == nil {
		return updated, nil
	}

	if !apierrors.IsConflict(err) {
		return t, err
	}

	// Retry once on conflict
	latest, err = client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return t, err
	}

	resource = modifier(latest)
	updated, err = client.UpdateStatus(ctx, resource, metav1.UpdateOptions{})
	if err == nil {
		return updated, nil
	}

	if !apierrors.IsConflict(err) {
		return t, err
	}

	// Retry again on conflict
	latest, err = client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return t, err
	}

	resource = modifier(latest)
	return client.UpdateStatus(ctx, resource, metav1.UpdateOptions{})
}
