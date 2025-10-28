# CIDN - Content Intelligent Distribution Network

CIDN is a Kubernetes-based distributed content delivery and caching system designed for high-performance, parallel downloads and uploads across multiple storage backends. It provides intelligent chunking, multipart upload management, and distributed task execution.

## Overview

CIDN orchestrates large file transfers by breaking them into chunks and distributing work across multiple runners. It supports multiple storage backends (S3, MinIO, etc.) and provides a web UI for monitoring transfer progress in real-time.

### Key Features

- **Distributed Downloads/Uploads**: Parallel chunk-based transfers across multiple runners
- **Multi-Backend Support**: Works with S3-compatible storage backends (MinIO, AWS S3, etc.)
- **Intelligent Chunking**: Automatic file splitting with configurable chunk sizes
- **Multipart Upload Management**: Handles large file uploads efficiently
- **Authentication Management**: Bearer token handling for authenticated sources
- **Real-time Monitoring**: Web UI showing transfer progress, speeds, and status
- **Kubernetes Native**: Built on Kubernetes API patterns with Custom Resource Definitions (CRDs)
- **High Availability**: Supports multiple replicas of all components

## Architecture

CIDN consists of four main components:

### 1. API Server
The central control plane that exposes a Kubernetes-compatible API for managing CIDN resources:
- **Blob**: Represents a complete file to be transferred
- **Chunk**: Represents a piece of a Blob (for parallel transfers)
- **Bearer**: Manages authentication tokens for protected resources
- **Multipart**: Coordinates multipart upload operations

### 2. Controller Manager
Orchestrates the lifecycle of CIDN resources:
- **Blob Controller**: Manages blob lifecycle and chunk creation
- **Chunk Controller**: Handles chunk state transitions
- **Bearer Controller**: Manages authentication tokens
- **Release Controllers**: Cleanup and resource management

### 3. Runner
Executes actual download/upload tasks:
- Processes chunks assigned to it
- Handles HTTP requests with resume capability
- Verifies checksums (SHA256)
- Reports progress back to API server

### 4. Web UI
Provides real-time monitoring:
- View all active transfers
- Monitor download/upload speeds
- Track progress with visual indicators
- View errors and retry status

## Installation

### Prerequisites

- Docker and Docker Compose
- Go 1.24.3 or later (for building from source)
- Kubernetes cluster (optional, for production deployment)

### Quick Start with Docker Compose

1. Clone the repository:
```bash
git clone https://github.com/OpenCIDN/cidn.git
cd cidn
```

2. Start all services:
```bash
docker compose up -d
```

This will start:
- etcd (for API server storage)
- MinIO (example storage backend, 2 replicas)
- API Server (port 6443)
- Controller Manager (2 replicas)
- Runners (8 replicas)
- Web UI (port 8080)

3. Access the Web UI:
```
http://localhost:8080
```

### Building from Source

```bash
# Build all components
make build

# Or build individual components
go build -o bin/apiserver ./cmd/apiserver
go build -o bin/controller-manager ./cmd/controller-manager
go build -o bin/runner ./cmd/runner
go build -o bin/webui ./cmd/webui
```

## Configuration

### API Server

The API server requires:
- `--etcd-servers`: etcd endpoints for storage
- `--authentication-token-webhook-config-file`: Authentication configuration
- `--authorization-mode`: Authorization mode (RBAC, Webhook, etc.)

Example:
```bash
./apiserver \
  --etcd-servers=http://etcd:2379 \
  --secure-port=6443
```

### Controller Manager

Key configuration options:
- `--master`: API server endpoint
- `--storage-url`: Storage backend URLs (can specify multiple)
- `--user`: User credentials for API access

Example:
```bash
./controller-manager \
  --master=https://admin:admin-pwd@apiserver:6443 \
  --insecure-skip-tls-verify \
  --storage-url=minio-1://minioadmin:minioadmin@myminio.us-east-1?forcepathstyle=true&secure=false&chunksize=104857600&regionendpoint=http://minio-1:9000 \
  --storage-url=minio-2://minioadmin:minioadmin@myminio.us-east-1?forcepathstyle=true&secure=false&chunksize=104857600&regionendpoint=http://minio-2:9000
```

Storage URL format:
```
<backend>://<access-key>:<secret-key>@<bucket>.<region>?<parameters>
```

Parameters:
- `forcepathstyle=true`: Use path-style URLs (required for MinIO)
- `secure=false`: Disable TLS
- `chunksize=<bytes>`: Chunk size for uploads
- `regionendpoint=<url>`: S3 endpoint URL

### Runner

Key configuration options:
- `--master`: API server endpoint
- `--handler-name`: Unique name for this runner instance

Example:
```bash
./runner \
  --master=https://runner:runner-pwd@apiserver:6443 \
  --insecure-skip-tls-verify
```

### Web UI

Key configuration options:
- `--master`: API server endpoint
- `--port`: HTTP port to listen on

Example:
```bash
./webui \
  --master=https://viewer:viewer-pwd@apiserver:6443 \
  --insecure-skip-tls-verify \
  --port=8080
```

## Usage

### Creating a Download Task

Create a Blob resource to download a file:

```yaml
apiVersion: task.cidn.io/v1alpha1
kind: Blob
metadata:
  name: example-download
spec:
  source:
    - url: "https://example.com/large-file.bin"
  destination:
    - name: "minio-1"
      path: "/downloads/large-file.bin"
    - name: "minio-2"
      path: "/downloads/large-file.bin"
  priority: 100
  chunkSize: 104857600  # 100MB chunks
  chunksNumber: 10
  maximumRunning: 3
  maximumPending: 1
  contentSha256: "sha256-hash-here"  # optional verification
```
