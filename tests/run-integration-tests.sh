#!/bin/bash
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

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}CIDN Integration Test Runner${NC}"
echo "=============================="

# Check if Docker and Docker Compose are available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    exit 1
fi

if ! docker compose version &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not available${NC}"
    exit 1
fi

# Parse command line arguments
SKIP_BUILD=false
CLEANUP=true
TEST_PATTERN=".*"

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
        --run)
            TEST_PATTERN="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --skip-build   Skip building Docker images (use existing images)"
            echo "  --no-cleanup   Don't stop services after tests (for debugging)"
            echo "  --run PATTERN  Run only tests matching PATTERN"
            echo "  --help         Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                           # Run all tests"
            echo "  $0 --run TestBlobLifecycle   # Run only blob lifecycle test"
            echo "  $0 --no-cleanup              # Run tests and leave services running"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Cleanup function
cleanup() {
    if [ "$CLEANUP" = true ]; then
        echo ""
        echo -e "${YELLOW}Cleaning up Docker Compose services...${NC}"
        cd "$PROJECT_ROOT"
        docker compose -f compose.local.yaml down -v
        echo -e "${GREEN}Cleanup complete${NC}"
    else
        echo ""
        echo -e "${YELLOW}Leaving services running (--no-cleanup was specified)${NC}"
        echo "To stop services manually, run:"
        echo "  cd $PROJECT_ROOT && docker compose -f compose.local.yaml down -v"
    fi
}

# Set trap to cleanup on script exit
if [ "$CLEANUP" = true ]; then
    trap cleanup EXIT
fi

# Navigate to project root
cd "$PROJECT_ROOT"

# Start services
echo ""
echo -e "${GREEN}Starting CIDN services with Docker Compose...${NC}"
if [ "$SKIP_BUILD" = true ]; then
    echo "Skipping build (using existing images)"
    docker compose -f compose.local.yaml up -d --no-build
else
    docker compose -f compose.local.yaml up -d --build
fi

# Wait for services to be ready
echo ""
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 5

# Check if API server is responding
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -sk https://admin:admin-pwd@localhost:6443/healthz &> /dev/null; then
        echo -e "${GREEN}API server is ready${NC}"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "${RED}Timeout waiting for API server to be ready${NC}"
        echo "Checking service status:"
        docker compose -f compose.local.yaml ps
        echo ""
        echo "API server logs:"
        docker compose -f compose.local.yaml logs apiserver
        exit 1
    fi
    echo -n "."
    sleep 2
done

echo ""
echo -e "${GREEN}Running integration tests...${NC}"
cd "$SCRIPT_DIR"

# Run the tests
if go test -v -timeout 10m -run "$TEST_PATTERN"; then
    echo ""
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}✗ Tests failed${NC}"
    echo ""
    echo "To debug, check the logs:"
    echo "  docker compose -f $PROJECT_ROOT/compose.local.yaml logs"
    exit 1
fi
