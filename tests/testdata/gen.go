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

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"html/template"
	"io"
	"log"
	"os"
	"strings"

	humanize "github.com/dustin/go-humanize"
)

var files = []string{
	"1b",
	"16b",
	"5kib",
	"5mib",
	"50mib",
	"100mib",
	"128mib",
	"256mib",
	"512mib",
	"1gib",
	"2gib",
	"3gib",
	"4gib",
	"5gib",
	"6gib",
	"7gib",
	"8gib",
	"9gib",
	"10gib",
}

func parseSizeExpression(expr string) (uint64, error) {
	return humanize.ParseBytes(expr)
}

var blobTmpl = template.Must(template.New("blob").Parse(`
apiVersion: task.opencidn.daocloud.io/v1alpha1
kind: Blob
metadata:
  name: blob.{{.Destination}}.1-1.hash-error
spec:
  minimumChunkSize: 200000000
  maximumRunning: 2
  contentSha256: {{.Sha256}}xx
  source:
  - url: http://cidn-nginx-test-1/{{.Source}}
  destination:
  - name: minio-1
    path: blob.{{.Destination}}.1-1.hash-error
---
apiVersion: task.opencidn.daocloud.io/v1alpha1
kind: Blob
metadata:
  name: blob.{{.Destination}}.1-1
spec:
  minimumChunkSize: 100000000
  maximumRunning: 2
  contentSha256: {{.Sha256}}
  source:
  - url: http://cidn-nginx-test-1/{{.Source}}
  destination:
  - name: minio-1
    path: blob.{{.Destination}}.1-1
    skipIfExists: true
---
apiVersion: task.opencidn.daocloud.io/v1alpha1
kind: Blob
metadata:
  name: blob.{{.Destination}}.2-2
spec:
  minimumChunkSize: 134217728
  maximumRunning: 2
  contentSha256: {{.Sha256}}
  source:
  - url: http://cidn-nginx-test-1/{{.Source}}
  - url: http://cidn-nginx-test-2/{{.Source}}
  destination:
  - name: minio-1
    path: blob.{{.Destination}}.2-2
    verifySha256: true
    skipIfExists: true
  - name: minio-2
    path: blob.{{.Destination}}.2-2
    verifySha256: true
    skipIfExists: true
---
`))

func buildBlob(sha256 string, source string, destination string) string {
	data := struct {
		Sha256      string
		Source      string
		Destination string
	}{
		Sha256:      sha256,
		Source:      source,
		Destination: destination,
	}

	var buf strings.Builder
	if err := blobTmpl.Execute(&buf, data); err != nil {
		return ""
	}
	return buf.String()
}

var randReader = rand.Reader

func main() {
	b, err := os.Create("blob.yaml")
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	for _, file := range files {
		size, err := parseSizeExpression(file)
		if err != nil {
			log.Fatal(err)
		}

		src := file + ".rand"
		f, err := os.Create(src)
		if err != nil {
			log.Fatal(err)
		}

		hash := sha256.New()
		n, err := io.CopyN(f, io.TeeReader(randReader, hash), int64(size))
		if err != nil {
			f.Close()
			log.Fatal(err)
		}
		if n != int64(size) {
			f.Close()
			log.Fatalf("expected to write %d bytes, but wrote %d bytes", size, n)
		}

		err = f.Close()
		if err != nil {
			log.Fatal(err)
		}

		_, err = b.WriteString(buildBlob(hex.EncodeToString(hash.Sum(nil)), src, file))
		if err != nil {
			log.Fatal(err)
		}
	}

	// Remove any .rand files not in our files list
	entries, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".rand") {
			continue
		}

		base := strings.TrimSuffix(name, ".rand")
		found := false
		for _, f := range files {
			if f == base {
				found = true
				break
			}
		}

		if !found {
			err := os.Remove(name)
			if err != nil {
				log.Printf("Failed to remove %s: %v", name, err)
			}
		}
	}
}
