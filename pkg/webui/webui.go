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

package webui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sync"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"k8s.io/client-go/tools/cache"
)

//go:embed html/*
var embedFS embed.FS

// Event represents a server-sent event for the WebUI
type Event struct {
	ID   string
	Type string
	Data []byte
}

// WriteTo writes the event in Server-Sent Events format to the provided writer
func (e *Event) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", e.ID, e.Type, e.Data)
	return int64(n), err
}

// NewHandler returns an http.Handler serving the WebUI and API endpoints
func NewHandler(client versioned.Interface, updateInterval time.Duration) http.Handler {
	sharedInformerFactory := externalversions.NewSharedInformerFactory(client, 0)
	mux := http.NewServeMux()

	subFS, err := fs.Sub(embedFS, "html")
	if err != nil {
		panic(fmt.Errorf("failed to get sub filesystem: %v", err))
	}

	// Serve static files
	mux.Handle("/", http.FileServer(http.FS(subFS)))

	// Add API endpoints
	blobInformer := sharedInformerFactory.Task().V1alpha1().Blobs()
	blobInf := blobInformer.Informer()
	go blobInf.RunWithContext(context.Background())

	chunkInformer := sharedInformerFactory.Task().V1alpha1().Chunks()
	chunkInf := chunkInformer.Informer()
	go chunkInf.RunWithContext(context.Background())

	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		// Set headers for Server-Sent Events
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Channel for direct updates
		updates := make(chan Event, 4)
		defer close(updates)

		// Buffer for aggregated updates
		var mut sync.Mutex
		updateBuffer := map[string]*Event{}
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		resourceEventHandlerRegistration, err := blobInf.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				event := createEvent("ADD", blob)
				updates <- event
				updateBuffer[string(blob.UID)] = nil
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldBlob, ok := oldObj.(*v1alpha1.Blob)
				if !ok {
					return
				}
				newBlob, ok := newObj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				event := createEvent("UPDATE", newBlob)
				_, ok = updateBuffer[string(newBlob.UID)]
				if ok && oldBlob.Status.Phase == newBlob.Status.Phase && newBlob.Status.Progress != newBlob.Status.Total {
					updateBuffer[string(newBlob.UID)] = &event
				} else {
					updateBuffer[string(newBlob.UID)] = nil
					updates <- event
				}
			},
			DeleteFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				delete(updateBuffer, string(blob.UID))
				event := createEvent("DELETE", blob)
				updates <- event
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to add event handler: %v", err), http.StatusInternalServerError)
			return
		}

		defer blobInf.RemoveEventHandler(resourceEventHandlerRegistration)

		chunkEventHandlerRegistration, err := chunkInf.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				chunk, ok := obj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				event := createChunkEvent("ADD", chunk)
				updates <- event
				updateBuffer[string(chunk.UID)] = nil
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldChunk, ok := oldObj.(*v1alpha1.Chunk)
				if !ok {
					return
				}
				newChunk, ok := newObj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				event := createChunkEvent("UPDATE", newChunk)
				_, ok = updateBuffer[string(newChunk.UID)]
				// Buffer intermediate progress updates for the same phase
				// Note: Chunk.Spec.Total vs Blob.Status.Total - chunks store total in spec
				if ok && oldChunk.Status.Phase == newChunk.Status.Phase && newChunk.Status.Progress != newChunk.Spec.Total {
					updateBuffer[string(newChunk.UID)] = &event
				} else {
					updateBuffer[string(newChunk.UID)] = nil
					updates <- event
				}
			},
			DeleteFunc: func(obj interface{}) {
				chunk, ok := obj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				delete(updateBuffer, string(chunk.UID))
				event := createChunkEvent("DELETE", chunk)
				updates <- event
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to add chunk event handler: %v", err), http.StatusInternalServerError)
			return
		}

		defer chunkInf.RemoveEventHandler(chunkEventHandlerRegistration)

		// Stream updates to client
		flusher := w.(http.Flusher)
		for {
			select {
			case <-ticker.C:
				func() {
					mut.Lock()
					defer mut.Unlock()
					if len(updateBuffer) != 0 {
						for _, event := range updateBuffer {
							if event == nil {
								continue
							}
							_, err := event.WriteTo(w)
							if err != nil {
								fmt.Printf("Error writing event: %v\n", err)
								return
							}
						}
						flusher.Flush()
						clear(updateBuffer)
					}
				}()
			case event := <-updates:
				_, err := event.WriteTo(w)
				if err != nil {
					fmt.Printf("Error writing event: %v\n", err)
					return
				}
				flusher.Flush()
			case <-r.Context().Done():
				return // Client disconnected
			}
		}
	})

	return mux
}

// createEvent constructs an Event for a given blob and event type
func createEvent(eventType string, blob *v1alpha1.Blob) Event {
	event := Event{
		Type: eventType,
		ID:   string(blob.UID),
	}
	if eventType != "DELETE" {
		data, err := json.Marshal(cleanBlobForWebUI(blob))
		if err != nil {
			fmt.Printf("Error marshaling blob data: %v\n", err)
			data = []byte("{}")
		}
		event.Data = data
	}
	return event
}

// cleanedBlob is a reduced view of Blob for the WebUI
// Only relevant fields are exposed
type cleanedBlob struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`

	Priority     int64 `json:"priority,omitempty"`
	Total        int64 `json:"total"`
	ChunksNumber int64 `json:"chunksNumber"`

	Phase           v1alpha1.BlobPhase `json:"phase"`
	Progress        int64              `json:"progress"`
	PendingChunks   int64              `json:"pendingChunks,omitempty"`
	RunningChunks   int64              `json:"runningChunks,omitempty"`
	SucceededChunks int64              `json:"succeededChunks,omitempty"`
	FailedChunks    int64              `json:"failedChunks,omitempty"`

	Errors []string `json:"errors,omitempty"`
}

// cleanBlobForWebUI extracts and normalizes fields for the WebUI
func cleanBlobForWebUI(blob *v1alpha1.Blob) *cleanedBlob {
	cleaned := &cleanedBlob{}

	// Set metadata
	cleaned.Name = blob.Name

	if blob.Annotations != nil {
		cleaned.DisplayName = blob.Annotations[v1alpha1.BlobDisplayNameAnnotation]
	}

	// Set spec fields
	cleaned.Priority = blob.Spec.Priority
	cleaned.Total = blob.Status.Total
	cleaned.ChunksNumber = blob.Spec.ChunksNumber

	// Set status fields
	cleaned.Phase = blob.Status.Phase
	cleaned.Progress = blob.Status.Progress
	cleaned.PendingChunks = blob.Status.PendingChunks
	cleaned.RunningChunks = blob.Status.RunningChunks
	cleaned.SucceededChunks = blob.Status.SucceededChunks
	cleaned.FailedChunks = blob.Status.FailedChunks

	if cleaned.Phase == v1alpha1.BlobPhaseRunning &&
		cleaned.Progress == 0 &&
		cleaned.RunningChunks == 0 &&
		cleaned.FailedChunks == 0 &&
		cleaned.SucceededChunks == 0 {
		cleaned.Phase = v1alpha1.BlobPhasePending
	}

	if len(blob.Status.Conditions) > 0 {
		cleaned.Errors = make([]string, 0, len(blob.Status.Conditions))
		for _, condition := range blob.Status.Conditions {
			cleaned.Errors = append(cleaned.Errors, fmt.Sprintf("%s: %s", condition.Type, condition.Message))
		}
	}

	return cleaned
}

// createChunkEvent constructs an Event for a given chunk and event type
func createChunkEvent(eventType string, chunk *v1alpha1.Chunk) Event {
	event := Event{
		Type: eventType,
		ID:   string(chunk.UID),
	}
	if eventType != "DELETE" {
		data, err := json.Marshal(cleanChunkForWebUI(chunk))
		if err != nil {
			fmt.Printf("Error marshaling chunk data: %v\n", err)
			data = []byte("{}")
		}
		event.Data = data
	}
	return event
}

// cleanedChunk is a reduced view of Chunk for the WebUI
// Only relevant fields are exposed
type cleanedChunk struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`

	Priority   int64 `json:"priority,omitempty"`
	Total      int64 `json:"total"`
	ChunkIndex int64 `json:"chunkIndex,omitempty"`

	Phase    v1alpha1.ChunkPhase `json:"phase"`
	Progress int64               `json:"progress,omitempty"`

	Errors []string `json:"errors,omitempty"`
}

// cleanChunkForWebUI extracts and normalizes fields for the WebUI
func cleanChunkForWebUI(chunk *v1alpha1.Chunk) *cleanedChunk {
	cleaned := &cleanedChunk{}

	// Set metadata
	cleaned.Name = chunk.Name

	if chunk.Annotations != nil {
		cleaned.DisplayName = chunk.Annotations[v1alpha1.ChunkDisplayNameAnnotation]
	}

	// Set spec fields
	cleaned.Priority = chunk.Spec.Priority
	cleaned.Total = chunk.Spec.Total
	cleaned.ChunkIndex = chunk.Spec.ChunkIndex

	// Set status fields
	cleaned.Phase = chunk.Status.Phase
	cleaned.Progress = chunk.Status.Progress

	if cleaned.Phase == v1alpha1.ChunkPhaseRunning &&
		cleaned.Progress == 0 {
		cleaned.Phase = v1alpha1.ChunkPhasePending
	}

	if len(chunk.Status.Conditions) > 0 {
		cleaned.Errors = make([]string, 0, len(chunk.Status.Conditions))
		for _, condition := range chunk.Status.Conditions {
			cleaned.Errors = append(cleaned.Errors, fmt.Sprintf("%s: %s", condition.Type, condition.Message))
		}
	}

	return cleaned
}
