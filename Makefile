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

.PHONY: help build test test-unit test-integration clean

# Default target
help:
	@echo "CIDN Makefile targets:"
	@echo "  make build              - Build all binaries"
	@echo "  make test               - Run all tests (unit + integration)"
	@echo "  make test-unit          - Run unit tests only"
	@echo "  make test-integration   - Run integration tests"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make docker-up          - Start Docker Compose services"
	@echo "  make docker-down        - Stop Docker Compose services"

# Build all binaries
build:
	@echo "Building all binaries..."
	go build -o bin/apiserver ./cmd/apiserver
	go build -o bin/controller-manager ./cmd/controller-manager
	go build -o bin/runner ./cmd/runner
	go build -o bin/webui ./cmd/webui
	@echo "Build complete. Binaries are in bin/"

# Run all tests
test: test-unit test-integration

# Run unit tests
test-unit:
	@echo "Running unit tests..."
	go test -v -short ./...

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	@cd tests && ./run-integration-tests.sh

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	@echo "Clean complete"

# Start Docker Compose services
docker-up:
	@echo "Starting Docker Compose services..."
	docker compose -f compose.local.yaml up -d --build

# Stop Docker Compose services
docker-down:
	@echo "Stopping Docker Compose services..."
	docker compose -f compose.local.yaml down -v
