#!/usr/bin/env bash

# Copyright 2025 The OpenCIDN Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

DIR="$(dirname "${BASH_SOURCE[0]}")"

ROOT_DIR="$(realpath "${DIR}/..")"

KUBE_VERSION=v0.33.2

function deepcopy-gen() {
  go run k8s.io/code-generator/cmd/deepcopy-gen@${KUBE_VERSION} "$@"
}

function defaulter-gen() {
  go run k8s.io/code-generator/cmd/defaulter-gen@${KUBE_VERSION} "$@"
}

function conversion-gen() {
  go run k8s.io/code-generator/cmd/conversion-gen@${KUBE_VERSION} "$@"
}

function client-gen() {
  go run k8s.io/code-generator/cmd/client-gen@${KUBE_VERSION} "$@"
}

function register-gen() {
  go run k8s.io/code-generator/cmd/register-gen@${KUBE_VERSION} "$@"
}

function lister-gen() {
  go run k8s.io/code-generator/cmd/lister-gen@${KUBE_VERSION} "$@"
}

function informer-gen() {
  go run k8s.io/code-generator/cmd/informer-gen@${KUBE_VERSION} "$@"
}

function openapi-gen() {
  go run k8s.io/kube-openapi/cmd/openapi-gen@v0.0.0-20250318190949-c8a335a9a2ff "$@"
}

# Clean up previously generated files
rm -f "${ROOT_DIR}/pkg/apis/task/v1alpha1/zz_generated."*
rm -rf "${ROOT_DIR}/pkg/clientset"
rm -rf "${ROOT_DIR}/pkg/listers" 
rm -rf "${ROOT_DIR}/pkg/informers"
rm -rf "${ROOT_DIR}/pkg/openapi"


# Generate deepcopy functions
deepcopy-gen \
  --output-file zz_generated.deepcopy.go \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1

# Generate defaulting functions
defaulter-gen \
  --output-file zz_generated.defaults.go \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1

# Generate clientset
client-gen \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  --output-dir "${ROOT_DIR}/pkg/clientset" \
  --output-pkg github.com/OpenCIDN/cidn/pkg/clientset \
  --clientset-name versioned \
  --input-base github.com/OpenCIDN/cidn/pkg/apis \
  --input task/v1alpha1 \
  --plural-exceptions "" \
  --prefers-protobuf=false

# Generate lister
lister-gen \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  --output-dir "${ROOT_DIR}/pkg/listers" \
  --output-pkg github.com/OpenCIDN/cidn/pkg/listers \
  --plural-exceptions "" \
  github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1

# Generate informer
informer-gen \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  --output-dir "${ROOT_DIR}/pkg/informers" \
  --output-pkg github.com/OpenCIDN/cidn/pkg/informers \
  --versioned-clientset-package github.com/OpenCIDN/cidn/pkg/clientset/versioned \
  --listers-package github.com/OpenCIDN/cidn/pkg/listers \
  --plural-exceptions "" \
  github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1

# Generate openapi
openapi-gen \
  --output-file zz_generated.openapi.go \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  --output-dir "${ROOT_DIR}/pkg/openapi" \
  --output-pkg k8s.io/github.com/OpenCIDN/cidn/pkg/openapi \
  --report-filename /dev/null \
  k8s.io/apimachinery/pkg/apis/meta/v1 \
  k8s.io/apimachinery/pkg/runtime \
  k8s.io/apimachinery/pkg/version \
  github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1

# Generate register
register-gen \
  --output-file zz_generated.register.go \
  --go-header-file "${ROOT_DIR}/hack/boilerplate.go.txt" \
  github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1
