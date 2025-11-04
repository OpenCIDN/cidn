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
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// getTTLDuration parses the TTL annotation from object metadata and returns the duration.
// The annotation value should be a valid duration string (e.g., "1h", "30m", "3600s").
func getTTLDuration(objectMeta metav1.ObjectMeta, annotationKey string) (time.Duration, bool) {
	ttlStr, ok := objectMeta.Annotations[annotationKey]
	if !ok || ttlStr == "" {
		return 0, false
	}

	ttl, err := time.ParseDuration(ttlStr)
	if err != nil {
		return 0, false
	}

	if ttl <= 0 {
		return 0, false
	}

	return ttl, true
}
