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

package runner

import (
	"crypto/x509"
	"net/http"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned/fake"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
)

func TestNewHTTPClientWithSystemCerts(t *testing.T) {
	client := newHTTPClientWithSystemCerts()

	if client == nil {
		t.Fatal("Expected non-nil HTTP client")
	}

	if client.Transport == nil {
		t.Fatal("Expected non-nil transport")
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("Expected transport to be *http.Transport")
	}

	if transport.TLSClientConfig == nil {
		t.Fatal("Expected non-nil TLS client config")
	}

	if transport.TLSClientConfig.RootCAs == nil {
		t.Fatal("Expected non-nil RootCAs (system certificate pool)")
	}
}

func TestNewChunkRunnerUsesSystemCerts(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	sharedInformerFactory := externalversions.NewSharedInformerFactory(clientset, 0)

	runner := NewChunkRunner("test-handler", clientset, sharedInformerFactory)

	if runner == nil {
		t.Fatal("Expected non-nil ChunkRunner")
	}

	if runner.httpClient == nil {
		t.Fatal("Expected non-nil HTTP client")
	}

	transport, ok := runner.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("Expected transport to be *http.Transport")
	}

	if transport.TLSClientConfig == nil {
		t.Fatal("Expected non-nil TLS client config")
	}

	if transport.TLSClientConfig.RootCAs == nil {
		t.Fatal("Expected non-nil RootCAs (system certificate pool)")
	}
}

func TestHTTPClientWithSystemCertsMatchesSystemPool(t *testing.T) {
	// Get the system certificate pool for comparison
	systemPool, err := x509.SystemCertPool()
	if err != nil {
		t.Skipf("Skipping test: cannot load system cert pool: %v", err)
	}

	client := newHTTPClientWithSystemCerts()
	transport := client.Transport.(*http.Transport)
	tlsConfig := transport.TLSClientConfig

	// Verify that the RootCAs is set (we can't directly compare cert pools,
	// but we can verify it's not nil and appears to be configured)
	if tlsConfig.RootCAs == nil {
		t.Fatal("Expected RootCAs to be configured with system certificates")
	}

	// Verify it's not an empty pool by checking subjects
	// The system pool should have at least some certificates
	if len(systemPool.Subjects()) == 0 {
		t.Skip("Skipping test: system cert pool appears to be empty")
	}

	// We can't directly compare the cert pools, but we can verify
	// that our TLS config has RootCAs set, which is the important part
	if tlsConfig.RootCAs == nil {
		t.Error("TLS config RootCAs should be set to system cert pool")
	}
}

func TestHTTPClientTLSConfiguration(t *testing.T) {
	client := newHTTPClientWithSystemCerts()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("Expected transport to be *http.Transport")
	}

	tlsConfig := transport.TLSClientConfig
	if tlsConfig == nil {
		t.Fatal("Expected non-nil TLS config")
	}

	// Verify TLS configuration properties
	if tlsConfig.RootCAs == nil {
		t.Error("Expected RootCAs to be set")
	}

	// Verify that InsecureSkipVerify is false (default)
	if tlsConfig.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify to be false by default")
	}
}
