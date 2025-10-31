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

// blobStore holds the current state of all blobs for group aggregation
type blobStore struct {
	mu    sync.RWMutex
	blobs map[string]*v1alpha1.Blob
}

func newBlobStore() *blobStore {
	return &blobStore{
		blobs: make(map[string]*v1alpha1.Blob),
	}
}

func (s *blobStore) set(blob *v1alpha1.Blob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs[string(blob.UID)] = blob
}

func (s *blobStore) delete(blob *v1alpha1.Blob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blobs, string(blob.UID))
}

func (s *blobStore) getGroupMembers(groupName string) []*v1alpha1.Blob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var members []*v1alpha1.Blob
	for _, blob := range s.blobs {
		if blob.Annotations != nil && blob.Annotations[v1alpha1.BlobGroupAnnotation] == groupName {
			members = append(members, blob)
		}
	}
	return members
}

// aggregateGroupBlob creates an aggregated view of a group
func aggregateGroupBlob(groupName string, members []*v1alpha1.Blob) *cleanedBlob {
	if len(members) == 0 {
		return nil
	}

	aggregate := &cleanedBlob{
		Name:        "group-" + groupName,
		DisplayName: groupName,
		Group:       groupName,
		IsGroup:     true,
		Members:     make([]string, 0, len(members)),
	}

	// Aggregate statistics
	var totalSize, totalProgress int64
	var totalChunks, pendingChunks, runningChunks, succeededChunks, failedChunks int64
	var allErrors []string
	
	// Determine overall phase - use the "worst" phase among members
	phaseOrder := map[v1alpha1.BlobPhase]int{
		v1alpha1.BlobPhaseSucceeded: 0,
		v1alpha1.BlobPhaseRunning:   1,
		v1alpha1.BlobPhasePending:   2,
		v1alpha1.BlobPhaseUnknown:   3,
		v1alpha1.BlobPhaseFailed:    4,
	}
	aggregatePhase := v1alpha1.BlobPhaseSucceeded
	maxPhaseOrder := 0

	for _, blob := range members {
		aggregate.Members = append(aggregate.Members, blob.Name)
		
		totalSize += blob.Status.Total
		totalProgress += blob.Status.Progress
		totalChunks += blob.Spec.ChunksNumber
		pendingChunks += blob.Status.PendingChunks
		runningChunks += blob.Status.RunningChunks
		succeededChunks += blob.Status.SucceededChunks
		failedChunks += blob.Status.FailedChunks

		// Collect errors
		for _, condition := range blob.Status.Conditions {
			allErrors = append(allErrors, fmt.Sprintf("%s: %s: %s", blob.Name, condition.Type, condition.Message))
		}

		// Determine phase
		phase := blob.Status.Phase
		if phase == v1alpha1.BlobPhaseRunning &&
			blob.Status.Progress == 0 &&
			blob.Status.RunningChunks == 0 &&
			blob.Status.FailedChunks == 0 &&
			blob.Status.SucceededChunks == 0 {
			phase = v1alpha1.BlobPhasePending
		}
		
		if order, ok := phaseOrder[phase]; ok && order > maxPhaseOrder {
			maxPhaseOrder = order
			aggregatePhase = phase
		}
	}

	aggregate.Total = totalSize
	aggregate.Progress = totalProgress
	aggregate.ChunksNumber = totalChunks
	aggregate.PendingChunks = pendingChunks
	aggregate.RunningChunks = runningChunks
	aggregate.SucceededChunks = succeededChunks
	aggregate.FailedChunks = failedChunks
	aggregate.Phase = aggregatePhase
	aggregate.Errors = allErrors

	return aggregate
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
	informer := blobInformer.Informer()
	go informer.RunWithContext(context.Background())

	// Store for group aggregation
	store := newBlobStore()

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

		resourceEventHandlerRegistration, err := informer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				store.set(blob)

				mut.Lock()
				defer mut.Unlock()
				event := createEvent("ADD", blob)
				updates <- event
				updateBuffer[string(blob.UID)] = nil

				// Send group event if blob belongs to a group
				if blob.Annotations != nil && blob.Annotations[v1alpha1.BlobGroupAnnotation] != "" {
					groupName := blob.Annotations[v1alpha1.BlobGroupAnnotation]
					members := store.getGroupMembers(groupName)
					groupBlob := aggregateGroupBlob(groupName, members)
					if groupBlob != nil {
						groupEvent := createGroupEvent("ADD", groupBlob)
						updates <- groupEvent
						updateBuffer["group-"+groupName] = nil
					}
				}
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

				store.set(newBlob)

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

				// Send group event if blob belongs to a group
				if newBlob.Annotations != nil && newBlob.Annotations[v1alpha1.BlobGroupAnnotation] != "" {
					groupName := newBlob.Annotations[v1alpha1.BlobGroupAnnotation]
					members := store.getGroupMembers(groupName)
					groupBlob := aggregateGroupBlob(groupName, members)
					if groupBlob != nil {
						groupEvent := createGroupEvent("UPDATE", groupBlob)
						_, ok = updateBuffer["group-"+groupName]
						if ok && oldBlob.Status.Phase == newBlob.Status.Phase && newBlob.Status.Progress != newBlob.Status.Total {
							updateBuffer["group-"+groupName] = &groupEvent
						} else {
							updateBuffer["group-"+groupName] = nil
							updates <- groupEvent
						}
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				groupName := ""
				if blob.Annotations != nil {
					groupName = blob.Annotations[v1alpha1.BlobGroupAnnotation]
				}

				store.delete(blob)

				mut.Lock()
				defer mut.Unlock()
				delete(updateBuffer, string(blob.UID))
				event := createEvent("DELETE", blob)
				updates <- event

				// Send group update/delete event if blob belonged to a group
				if groupName != "" {
					members := store.getGroupMembers(groupName)
					if len(members) > 0 {
						groupBlob := aggregateGroupBlob(groupName, members)
						if groupBlob != nil {
							groupEvent := createGroupEvent("UPDATE", groupBlob)
							updates <- groupEvent
							updateBuffer["group-"+groupName] = nil
						}
					} else {
						// No members left, delete the group
						delete(updateBuffer, "group-"+groupName)
						groupDeleteEvent := Event{
							ID:   "group-" + groupName,
							Type: "DELETE",
						}
						updates <- groupDeleteEvent
					}
				}
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
		data, _ := json.Marshal(cleanBlobForWebUI(blob))
		event.Data = data
	}
	return event
}

// createGroupEvent constructs an Event for a group aggregate
func createGroupEvent(eventType string, groupBlob *cleanedBlob) Event {
	event := Event{
		Type: eventType,
		ID:   groupBlob.Name,
	}
	if eventType != "DELETE" {
		data, _ := json.Marshal(groupBlob)
		event.Data = data
	}
	return event
}

// cleanedBlob is a reduced view of Blob for the WebUI
// Only relevant fields are exposed
type cleanedBlob struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	Group       string `json:"group,omitempty"`

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

	// IsGroup indicates this is an aggregated group entry
	IsGroup bool `json:"isGroup,omitempty"`
	// Members contains the names of blobs that are part of this group
	Members []string `json:"members,omitempty"`
}

// cleanBlobForWebUI extracts and normalizes fields for the WebUI
func cleanBlobForWebUI(blob *v1alpha1.Blob) *cleanedBlob {
	cleaned := &cleanedBlob{}

	// Set metadata
	cleaned.Name = blob.Name

	if blob.Annotations != nil {
		cleaned.DisplayName = blob.Annotations[v1alpha1.BlobDisplayNameAnnotation]
		cleaned.Group = blob.Annotations[v1alpha1.BlobGroupAnnotation]
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
