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

package tests

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	taskv1alpha1 "github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// S3's minimum multipart part size.
	minimumChunkSize = 5 << 20
	// Far more retries than nginx-test's injected failures use up.
	maximumRetry = 10
	blobTimeout  = 3 * time.Minute
	apiTimeout   = 30 * time.Second
)

func TestBlobSingleChunkSucceeds(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-single")
	sha := writeSource(t, name, 1<<20)
	createBlob(t, client, name, sha)

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	if blob.Spec.ChunksNumber != 1 {
		t.Errorf("blob %s was split into %d chunks, want 1", name, blob.Spec.ChunksNumber)
	}
}

func TestBlobMultiChunkSucceeds(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-multi")
	sha := writeSource(t, name, 16<<20)
	createBlob(t, client, name, sha)

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseSucceeded {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseSucceeded, statusJSON(blob))
	}
	// Chunks are deleted once the Blob succeeds, so only the Blob tells how it was split.
	if blob.Spec.ChunksNumber < 2 {
		t.Errorf("blob %s was split into %d chunks, want more than 1", name, blob.Spec.ChunksNumber)
	}
}

func TestBlobSha256MismatchFails(t *testing.T) {
	t.Parallel()
	client := newE2EClient(t)
	name := uniqueName("e2e-mismatch")
	writeSource(t, name, 1<<20)
	createBlob(t, client, name, strings.Repeat("0", 64))

	blob := waitForBlobDone(t, client, name)
	if blob.Status.Phase != taskv1alpha1.BlobPhaseFailed {
		t.Fatalf("blob %s is %s, want %s; status: %s", name, blob.Status.Phase, taskv1alpha1.BlobPhaseFailed, statusJSON(blob))
	}
	for _, condition := range blob.Status.Conditions {
		if strings.Contains(condition.Message, "sha256 mismatch") {
			return
		}
	}
	t.Errorf("blob %s failed without a sha256 mismatch condition; status: %s", name, statusJSON(blob))
}

// newE2EClient skips in short mode and fails when the API server is unreachable.
func newE2EClient(t *testing.T) versioned.Interface {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config, err := getClientConfig()
	if err != nil {
		t.Fatalf("failed to get client config: %v", err)
	}
	if err := waitForAPIServer(t, config); err != nil {
		t.Fatalf("API server not available: %v (use docker compose up to start services)", err)
	}
	client, err := versioned.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

func uniqueName(prefix string) string {
	b := make([]byte, 4)
	rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// writeSource fills testdata/<name>.rand, which nginx-test serves, with random bytes and returns their sha256.
func writeSource(t *testing.T, name string, size int64) string {
	t.Helper()
	path := filepath.Join("testdata", name+".rand")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil {
			t.Errorf("failed to remove source file: %v", err)
		}
	})

	hash := sha256.New()
	_, err = io.CopyN(io.MultiWriter(f, hash), rand.Reader, size)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}
	// nginx-test reads the file as another user, whatever the umask.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("failed to make source file readable: %v", err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// createBlob copies testdata/<name>.rand from nginx-test to minio-1, verifying it against sha; edits adjust the spec first.
func createBlob(t *testing.T, client versioned.Interface, name, sha string, edits ...func(*taskv1alpha1.BlobSpec)) {
	t.Helper()
	blob := &taskv1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: taskv1alpha1.BlobSpec{
			Source: []taskv1alpha1.BlobSource{{
				URL:        "http://cidn-nginx-test-1/" + name + ".rand",
				BearerName: name,
			}},
			Destination: []taskv1alpha1.BlobDestination{{
				Name:           "minio-1",
				Path:           "e2e/" + name,
				ReverifySha256: true,
			}},
			MinimumChunkSize: minimumChunkSize,
			ContentSha256:    sha,
			MaximumRetry:     maximumRetry,
		},
	}
	for _, edit := range edits {
		edit(&blob.Spec)
	}

	ctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
	defer cancel()
	if _, err := client.TaskV1alpha1().Blobs().Create(ctx, blob, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create blob %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
		defer cancel()
		if err := client.TaskV1alpha1().Blobs().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Errorf("failed to delete blob %s: %v", name, err)
		}
	})
}

// waitForBlobDone returns the Blob once it is Succeeded or Failed.
func waitForBlobDone(t *testing.T, client versioned.Interface, name string) *taskv1alpha1.Blob {
	t.Helper()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), blobTimeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var last *taskv1alpha1.Blob
	var lastErr error
	for {
		blob, err := client.TaskV1alpha1().Blobs().Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			last, lastErr = blob, nil
			if blob.Status.Phase == taskv1alpha1.BlobPhaseSucceeded || blob.Status.Phase == taskv1alpha1.BlobPhaseFailed {
				t.Logf("blob %s is %s after %s", name, blob.Status.Phase, time.Since(start).Round(time.Second))
				return blob
			}
		} else if ctx.Err() == nil {
			// A Get cut short by the deadline says nothing about the API server.
			lastErr = err
		}

		select {
		case <-ctx.Done():
			t.Fatalf("blob %s did not finish within %s; status: %s; last get error: %v", name, blobTimeout, statusJSON(last), lastErr)
		case <-ticker.C:
		}
	}
}

func statusJSON(blob *taskv1alpha1.Blob) string {
	if blob == nil {
		return "never read"
	}
	data, _ := json.Marshal(blob.Status)
	return string(data)
}
