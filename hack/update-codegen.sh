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

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${SCRIPT_DIR}/kube_codegen.sh"

THIS_PKG="github.com/OpenCIDN/cidn"

kube::codegen::gen_helpers \
    --boilerplate "${ROOT_DIR}/hack/boilerplate.go.txt" \
    "${ROOT_DIR}/pkg/apis"

kube::codegen::gen_client \
    --with-watch \
    --output-dir "${ROOT_DIR}/pkg" \
    --output-pkg "${THIS_PKG}/pkg" \
    --boilerplate "${ROOT_DIR}/hack/boilerplate.go.txt" \
    "${ROOT_DIR}/pkg/apis"

kube::codegen::gen_openapi \
    --output-dir "${ROOT_DIR}/pkg/openapi" \
    --output-pkg "k8s.io/${THIS_PKG}/pkg/openapi" \
    --report-filename "${report_filename:-"/dev/null"}" \
    --update-report \
    --boilerplate "${ROOT_DIR}/hack/boilerplate.go.txt" \
    "${ROOT_DIR}/pkg/apis"

kube::codegen::gen_register \
    --boilerplate "${ROOT_DIR}/hack/boilerplate.go.txt" \
    "${ROOT_DIR}/pkg/apis"
