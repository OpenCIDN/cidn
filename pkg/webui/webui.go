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
func NewHandler(client versioned.Interface) http.Handler {
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
	informer := blobInformer.Informer()
	go informer.RunWithContext(context.Background())

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
		updateBuffer := make(map[string]Event)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		resourceEventHandlerRegistration, err := informer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}
				event := createEvent("ADD", blob)
				updates <- event
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
				if oldBlob.Status.Phase == newBlob.Status.Phase &&
					oldBlob.Status.FailedChunks == newBlob.Status.FailedChunks &&
					oldBlob.Status.PendingChunks == newBlob.Status.PendingChunks &&
					oldBlob.Status.RunningChunks == newBlob.Status.RunningChunks &&
					oldBlob.Status.SucceededChunks == newBlob.Status.SucceededChunks {
					updateBuffer[string(newBlob.UID)] = createEvent("UPDATE", newBlob)
				} else {
					delete(updateBuffer, string(newBlob.UID))
					event := createEvent("UPDATE", newBlob)
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

		defer informer.RemoveEventHandler(resourceEventHandlerRegistration)

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
		data, _ := json.Marshal(cleanBlobForWebUI(blob))
		event.Data = data
	}
	return event
}

// cleanedBlob is a reduced view of Blob for the WebUI
// Only relevant fields are exposed
type cleanedBlob struct {
	Name string `json:"name"`

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
	// Set spec fields
	cleaned.Priority = blob.Spec.Priority
	cleaned.Total = blob.Spec.Total
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
			if condition.Message != "" {
				cleaned.Errors = append(cleaned.Errors, condition.Message)
			} else if condition.Reason != "" {
				cleaned.Errors = append(cleaned.Errors, condition.Reason)
			} else if condition.Type != "" {
				cleaned.Errors = append(cleaned.Errors, condition.Type)
			}
		}
	}

	return cleaned
}
