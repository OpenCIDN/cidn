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
	"os"
	"sort"
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
		updateBuffer := map[string]*Event{}
		// Track blobs by group for aggregation
		groupBlobs := map[string]map[string]*v1alpha1.Blob{}
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		// Helper function to delete a group aggregate
		deleteGroupAggregate := func(group string) {
			if group == "" {
				return
			}
			delete(groupBlobs, group)
			delete(updateBuffer, "group-"+group)
			event := Event{Type: "DELETE", ID: "group-" + group}
			updates <- event
		}

		// Helper function to update group aggregate
		updateGroupAggregate := func(group string) {
			if group == "" {
				return
			}
			blobs, ok := groupBlobs[group]
			if !ok {
				return
			}

			aggregate := aggregateBlobs(group, blobs)
			event := Event{
				Type: "UPDATE",
				ID:   "group-" + group,
			}
			data, err := json.Marshal(aggregate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error marshaling group aggregate: %v\n", err)
				return
			}
			event.Data = data
			updateBuffer["group-"+group] = &event
		}

		// Helper function to remove blob from group and update or delete group aggregate
		removeFromGroup := func(group string, blobUID string) {
			if group == "" {
				return
			}
			// Check if group exists before attempting to modify
			if _, exists := groupBlobs[group]; !exists {
				return
			}
			delete(groupBlobs[group], blobUID)
			if len(groupBlobs[group]) == 0 {
				deleteGroupAggregate(group)
			} else {
				updateGroupAggregate(group)
			}
		}

		resourceEventHandlerRegistration, err := informer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
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

				// Track group membership
				if blob.Annotations != nil {
					if group := blob.Annotations[v1alpha1.BlobGroupAnnotation]; group != "" {
						if groupBlobs[group] == nil {
							groupBlobs[group] = make(map[string]*v1alpha1.Blob)
						}
						groupBlobs[group][string(blob.UID)] = blob
						updateGroupAggregate(group)
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

				// Update group membership
				oldGroup := ""
				newGroup := ""
				if oldBlob.Annotations != nil {
					oldGroup = oldBlob.Annotations[v1alpha1.BlobGroupAnnotation]
				}
				if newBlob.Annotations != nil {
					newGroup = newBlob.Annotations[v1alpha1.BlobGroupAnnotation]
				}

				// Remove from old group if changed
				if oldGroup != "" && oldGroup != newGroup {
					removeFromGroup(oldGroup, string(newBlob.UID))
				}

				// Add to new group
				if newGroup != "" {
					if groupBlobs[newGroup] == nil {
						groupBlobs[newGroup] = make(map[string]*v1alpha1.Blob)
					}
					groupBlobs[newGroup][string(newBlob.UID)] = newBlob
					updateGroupAggregate(newGroup)
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

				// Remove from group
				if blob.Annotations != nil {
					if group := blob.Annotations[v1alpha1.BlobGroupAnnotation]; group != "" {
						removeFromGroup(group, string(blob.UID))
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

// memberInfo holds information about a member blob in a group
type memberInfo struct {
	UID         string             `json:"uid"`
	DisplayName string             `json:"displayName"`
	Phase       v1alpha1.BlobPhase `json:"phase"`
}

// cleanedBlob is a reduced view of Blob for the WebUI
// Only relevant fields are exposed
type cleanedBlob struct {
	Name        string       `json:"name"`
	DisplayName string       `json:"displayName,omitempty"`
	Group       string       `json:"group,omitempty"`
	Members     []memberInfo `json:"members,omitempty"` // For group aggregates: list of member blob info

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

// aggregateBlobs creates an aggregate cleanedBlob from multiple blobs in a group
func aggregateBlobs(groupName string, blobs map[string]*v1alpha1.Blob) *cleanedBlob {
	aggregate := &cleanedBlob{
		Name:        groupName,
		DisplayName: groupName,
		Group:       groupName,
		Members:     make([]memberInfo, 0, len(blobs)),
	}

	var totalSize int64
	var totalProgress int64
	var totalChunks int64
	var pendingChunks int64
	var runningChunks int64
	var succeededChunks int64
	var failedChunks int64
	var maxPriority int64

	for blobUID, blob := range blobs {
		// Add member info to the list
		displayName := blob.Name
		if blob.Annotations != nil {
			if dn := blob.Annotations[v1alpha1.BlobDisplayNameAnnotation]; dn != "" {
				displayName = dn
			}
		}

		phase := blob.Status.Phase
		if phase == v1alpha1.BlobPhaseRunning &&
			blob.Status.Progress == 0 &&
			blob.Status.RunningChunks == 0 &&
			blob.Status.FailedChunks == 0 &&
			blob.Status.SucceededChunks == 0 {
			phase = v1alpha1.BlobPhasePending
		}

		aggregate.Members = append(aggregate.Members, memberInfo{
			UID:         string(blobUID),
			DisplayName: displayName,
			Phase:       phase,
		})

		totalSize += blob.Status.Total
		totalProgress += blob.Status.Progress
		totalChunks += blob.Spec.ChunksNumber
		pendingChunks += blob.Status.PendingChunks
		runningChunks += blob.Status.RunningChunks
		succeededChunks += blob.Status.SucceededChunks
		failedChunks += blob.Status.FailedChunks

		if blob.Spec.Priority > maxPriority {
			maxPriority = blob.Spec.Priority
		}
	}

	sort.Slice(aggregate.Members, func(i, j int) bool {
		return aggregate.Members[i].DisplayName < aggregate.Members[j].DisplayName
	})

	aggregate.Total = totalSize
	aggregate.Progress = totalProgress
	aggregate.ChunksNumber = totalChunks
	aggregate.PendingChunks = pendingChunks
	aggregate.RunningChunks = runningChunks
	aggregate.SucceededChunks = succeededChunks
	aggregate.FailedChunks = failedChunks
	aggregate.Priority = maxPriority

	switch {
	case succeededChunks == totalChunks && totalChunks > 0:
		aggregate.Phase = v1alpha1.BlobPhaseSucceeded
	case failedChunks > 0 && pendingChunks == 0 && runningChunks == 0:
		aggregate.Phase = v1alpha1.BlobPhaseFailed
	case totalProgress > 0:
		aggregate.Phase = v1alpha1.BlobPhaseRunning
	default:
		aggregate.Phase = v1alpha1.BlobPhasePending
	}

	return aggregate
}
