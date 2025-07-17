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
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	humanize "github.com/dustin/go-humanize"
)

var files = []string{
	"1b",
	"5kib",
	"5mib",
	"50mib",
	"100mib",
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

func buildBlob(sha256 string, source string, destination string) string {
	return fmt.Sprintf(
		`apiVersion: task.opencidn.daocloud.io/v1alpha1
kind: Blob
metadata:
  name: blob-%s
spec:
  minimumChunkSize: 268435456
  maximumParallelism: 2
  sha256: %s
  source: http://nginx-test/%s
  destination:
  - blob-%s
---
`, destination, sha256, source, destination)
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
