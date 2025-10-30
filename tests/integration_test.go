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
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	taskv1alpha1 "github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// getClientConfig returns a REST config for connecting to the CIDN API server.
// It checks for CIDN_MASTER environment variable, or uses a default value.
func getClientConfig() (*rest.Config, error) {
	master := os.Getenv("CIDN_MASTER")
	if master == "" {
		master = "https://admin:admin-pwd@localhost:6443"
	}

	config := &rest.Config{
		Host: master,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	return config, nil
}

// waitForAPIServer waits for the API server to be available
func waitForAPIServer(t *testing.T, config *rest.Config) error {
	t.Helper()

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for API server to be ready")
		case <-ticker.C:
			resp, err := client.Get(config.Host + "/healthz")
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				t.Log("API server is ready")
				return nil
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

// TestAPIServerHealth tests that the API server is healthy and accessible
func TestAPIServerHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config, err := getClientConfig()
	if err != nil {
		t.Fatalf("failed to get client config: %v", err)
	}

	err = waitForAPIServer(t, config)
	if err != nil {
		t.Skipf("API server not available: %v (use docker compose up to start services)", err)
	}

	t.Log("API server health check passed")
}

// TestBlobLifecycle tests the complete lifecycle of a Blob resource
func TestBlobLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config, err := getClientConfig()
	if err != nil {
		t.Fatalf("failed to get client config: %v", err)
	}

	err = waitForAPIServer(t, config)
	if err != nil {
		t.Skipf("API server not available: %v (use docker compose up to start services)", err)
	}

	client, err := versioned.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	blobName := fmt.Sprintf("test-blob-%d", time.Now().Unix())

	// Create a test blob
	blob := &taskv1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{
			Name: blobName,
		},
		Spec: taskv1alpha1.BlobSpec{
			Source: []taskv1alpha1.BlobSource{
				{
					URL: "http://cidn-nginx-test-1/1b.rand",
				},
			},
			Destination: []taskv1alpha1.BlobDestination{
				{
					Name: "minio-1",
					Path: fmt.Sprintf("/test-blobs/%s", blobName),
				},
			},
			MinimumChunkSize: 1024,
			MaximumRunning:   1,
		},
	}

	t.Logf("Creating blob: %s", blobName)
	createdBlob, err := client.TaskV1alpha1().Blobs().Create(ctx, blob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create blob: %v", err)
	}

	// Verify blob was created
	if createdBlob.Name != blobName {
		t.Errorf("expected blob name %s, got %s", blobName, createdBlob.Name)
	}
	t.Logf("Blob created successfully: %s", createdBlob.Name)

	// Clean up: delete the blob
	defer func() {
		t.Logf("Cleaning up blob: %s", blobName)
		err := client.TaskV1alpha1().Blobs().Delete(ctx, blobName, metav1.DeleteOptions{})
		if err != nil {
			t.Logf("warning: failed to delete blob: %v", err)
		}
	}()

	// Get the blob to verify it exists
	getBlob, err := client.TaskV1alpha1().Blobs().Get(ctx, blobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get blob: %v", err)
	}
	t.Logf("Retrieved blob: %s, Phase: %s", getBlob.Name, getBlob.Status.Phase)

	// List blobs to verify it appears in the list
	blobList, err := client.TaskV1alpha1().Blobs().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list blobs: %v", err)
	}

	found := false
	for _, b := range blobList.Items {
		if b.Name == blobName {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("created blob %s not found in blob list", blobName)
	}
	t.Logf("Blob list contains %d items, test blob found: %v", len(blobList.Items), found)
}

// TestChunkListing tests listing chunks (if any exist)
func TestChunkListing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config, err := getClientConfig()
	if err != nil {
		t.Fatalf("failed to get client config: %v", err)
	}

	err = waitForAPIServer(t, config)
	if err != nil {
		t.Skipf("API server not available: %v (use docker compose up to start services)", err)
	}

	client, err := versioned.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()

	// List chunks
	chunkList, err := client.TaskV1alpha1().Chunks().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list chunks: %v", err)
	}

	t.Logf("Found %d chunks in the system", len(chunkList.Items))
}
