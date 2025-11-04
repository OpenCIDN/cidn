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
	blobInformerInstance := blobInformer.Informer()
	go blobInformerInstance.RunWithContext(context.Background())

	chunkInformer := sharedInformerFactory.Task().V1alpha1().Chunks()
	chunkInformerInstance := chunkInformer.Informer()
	go chunkInformerInstance.RunWithContext(context.Background())

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
		// Track group for aggregation
		groups := map[string]map[string]*entry{}
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		// Helper function to delete a group aggregate
		deleteGroupAggregate := func(group string) {
			if group == "" {
				return
			}
			delete(groups, group)
			delete(updateBuffer, "group-"+group)
			event := Event{Type: "DELETE", ID: "group-" + group}
			updates <- event
		}

		// Helper function to update group aggregate
		updateGroupAggregate := func(group string, blobUID string, e *entry) {
			if group == "" {
				return
			}

			if groups[group] == nil {
				groups[group] = make(map[string]*entry)
			}

			if e != nil {
				groups[group][blobUID] = e
			}

			aggregate := aggregateEntries(group, groups[group])
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
			if _, exists := groups[group]; !exists {
				return
			}

			delete(groups[group], blobUID)
			if len(groups[group]) == 0 {
				deleteGroupAggregate(group)
			} else {
				updateGroupAggregate(group, blobUID, nil)
			}
		}

		resourceEventHandlerRegistration, err := blobInformerInstance.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				if blob.Annotations == nil {
					return
				}

				displayName := blob.Annotations[v1alpha1.BlobDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				e := blobToEntry(blob)
				event := createEvent("ADD", string(blob.UID), e)
				updates <- event
				updateBuffer[string(blob.UID)] = nil

				// Track group membership (supports multiple groups)
				if groupAnnotation := blob.Annotations[v1alpha1.BlobGroupAnnotation]; groupAnnotation != "" {
					groups := v1alpha1.ParseGroups(groupAnnotation)
					for _, group := range groups {
						updateGroupAggregate(group, string(blob.UID), e)
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

				if oldBlob.Annotations == nil {
					return
				}

				if newBlob.Annotations == nil {
					return
				}

				displayName := newBlob.Annotations[v1alpha1.BlobDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				e := blobToEntry(newBlob)
				event := createEvent("UPDATE", string(newBlob.UID), e)
				_, ok = updateBuffer[string(newBlob.UID)]
				if ok && oldBlob.Status.Phase == newBlob.Status.Phase && newBlob.Status.Progress != newBlob.Status.Total {
					updateBuffer[string(newBlob.UID)] = &event
				} else {
					updateBuffer[string(newBlob.UID)] = nil
					updates <- event
				}

				// Update group membership (supports multiple groups)
				oldGroupAnnotation := oldBlob.Annotations[v1alpha1.BlobGroupAnnotation]
				newGroupAnnotation := newBlob.Annotations[v1alpha1.BlobGroupAnnotation]

				oldGroups := v1alpha1.ParseGroups(oldGroupAnnotation)
				newGroups := v1alpha1.ParseGroups(newGroupAnnotation)

				// Create maps for efficient lookup
				oldGroupsMap := make(map[string]bool)
				for _, g := range oldGroups {
					oldGroupsMap[g] = true
				}
				newGroupsMap := make(map[string]bool)
				for _, g := range newGroups {
					newGroupsMap[g] = true
				}

				// Remove from groups that are no longer present
				for _, group := range oldGroups {
					if !newGroupsMap[group] {
						removeFromGroup(group, string(newBlob.UID))
					}
				}

				// Add to new groups
				for _, group := range newGroups {
					if oldGroupsMap[group] {
						// Already in group, just update
						updateGroupAggregate(group, string(newBlob.UID), e)
					} else {
						// New group membership
						updateGroupAggregate(group, string(newBlob.UID), e)
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				if blob.Annotations == nil {
					return
				}

				displayName := blob.Annotations[v1alpha1.BlobDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				delete(updateBuffer, string(blob.UID))
				e := blobToEntry(blob)
				event := createEvent("DELETE", string(blob.UID), e)
				updates <- event

				// Remove from all groups
				if groupAnnotation := blob.Annotations[v1alpha1.BlobGroupAnnotation]; groupAnnotation != "" {
					groups := v1alpha1.ParseGroups(groupAnnotation)
					for _, group := range groups {
						removeFromGroup(group, string(blob.UID))
					}
				}
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to add event handler: %v", err), http.StatusInternalServerError)
			return
		}
		defer blobInformerInstance.RemoveEventHandler(resourceEventHandlerRegistration)

		// Add Chunk event handlers
		chunkEventHandlerRegistration, err := chunkInformerInstance.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				chunk, ok := obj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				if chunk.Annotations == nil {
					return
				}

				displayName := chunk.Annotations[v1alpha1.ChunkDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				e := chunkToEntry(chunk)
				event := createEvent("ADD", string(chunk.UID), e)
				updates <- event
				updateBuffer[string(chunk.UID)] = nil

				// Track group membership (supports multiple groups)
				if groupAnnotation := chunk.Annotations[v1alpha1.ChunkGroupAnnotation]; groupAnnotation != "" {
					groups := v1alpha1.ParseGroups(groupAnnotation)
					for _, group := range groups {
						updateGroupAggregate(group, string(chunk.UID), e)
					}
				}
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

				if oldChunk.Annotations == nil {
					return
				}

				if newChunk.Annotations == nil {
					return
				}

				displayName := newChunk.Annotations[v1alpha1.ChunkDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				e := chunkToEntry(newChunk)
				event := createEvent("UPDATE", string(newChunk.UID), e)
				_, ok = updateBuffer[string(newChunk.UID)]
				if ok && oldChunk.Status.Phase == newChunk.Status.Phase && newChunk.Status.Progress != newChunk.Spec.Total {
					updateBuffer[string(newChunk.UID)] = &event
				} else {
					updateBuffer[string(newChunk.UID)] = nil
					updates <- event
				}

				// Update group membership (supports multiple groups)
				oldGroupAnnotation := oldChunk.Annotations[v1alpha1.ChunkGroupAnnotation]
				newGroupAnnotation := newChunk.Annotations[v1alpha1.ChunkGroupAnnotation]

				oldGroups := v1alpha1.ParseGroups(oldGroupAnnotation)
				newGroups := v1alpha1.ParseGroups(newGroupAnnotation)

				// Create maps for efficient lookup
				oldGroupsMap := make(map[string]bool)
				for _, g := range oldGroups {
					oldGroupsMap[g] = true
				}
				newGroupsMap := make(map[string]bool)
				for _, g := range newGroups {
					newGroupsMap[g] = true
				}

				// Remove from groups that are no longer present
				for _, group := range oldGroups {
					if !newGroupsMap[group] {
						removeFromGroup(group, string(newChunk.UID))
					}
				}

				// Add to new groups
				for _, group := range newGroups {
					if oldGroupsMap[group] {
						// Already in group, just update
						updateGroupAggregate(group, string(newChunk.UID), e)
					} else {
						// New group membership
						updateGroupAggregate(group, string(newChunk.UID), e)
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				chunk, ok := obj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				if chunk.Annotations == nil {
					return
				}

				displayName := chunk.Annotations[v1alpha1.ChunkDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				mut.Lock()
				defer mut.Unlock()
				delete(updateBuffer, string(chunk.UID))
				e := chunkToEntry(chunk)
				event := createEvent("DELETE", string(chunk.UID), e)
				updates <- event

				// Remove from all groups
				if groupAnnotation := chunk.Annotations[v1alpha1.ChunkGroupAnnotation]; groupAnnotation != "" {
					groups := v1alpha1.ParseGroups(groupAnnotation)
					for _, group := range groups {
						removeFromGroup(group, string(chunk.UID))
					}
				}
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to add chunk event handler: %v", err), http.StatusInternalServerError)
			return
		}
		defer chunkInformerInstance.RemoveEventHandler(chunkEventHandlerRegistration)

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
func createEvent(eventType string, id string, e *entry) Event {
	event := Event{
		Type: eventType,
		ID:   id,
	}
	if eventType != "DELETE" {
		data, _ := json.Marshal(e)
		event.Data = data
	}
	return event
}

// entryMemberInfo holds information about a member (blob or chunk) in a group
type entryMemberInfo struct {
	UID         string `json:"uid"`
	DisplayName string `json:"displayName"`
	Phase       string `json:"phase"`
}

// entry is a unified view of Blob or Chunk for the WebUI
// Only relevant fields are exposed
type entry struct {
	Name        string            `json:"name"`
	DisplayName string            `json:"displayName,omitempty"`
	Group       string            `json:"group,omitempty"`       // Deprecated: Use Groups instead. Kept for backward compatibility.
	Groups      []string          `json:"groups,omitempty"`      // List of groups this entry belongs to
	Members     []entryMemberInfo `json:"members,omitempty"`     // For group aggregates: list of member info

	Priority     int64 `json:"priority,omitempty"`
	Total        int64 `json:"total"`
	ChunksNumber int64 `json:"chunksNumber"`

	Phase           string `json:"phase"`
	Progress        int64  `json:"progress"`
	PendingChunks   int64  `json:"pendingChunks,omitempty"`
	RunningChunks   int64  `json:"runningChunks,omitempty"`
	SucceededChunks int64  `json:"succeededChunks,omitempty"`
	FailedChunks    int64  `json:"failedChunks,omitempty"`

	Errors []string `json:"errors,omitempty"`

	groupIgnoreSize bool `json:"-"`
}

// blobToEntry converts a Blob to an entry for the WebUI
func blobToEntry(blob *v1alpha1.Blob) *entry {
	e := &entry{}

	// Set metadata
	e.Name = blob.Name
	e.DisplayName = blob.Name
	if blob.Annotations != nil {
		if dn := blob.Annotations[v1alpha1.BlobDisplayNameAnnotation]; dn != "" {
			e.DisplayName = dn
		}
		groupAnnotation := blob.Annotations[v1alpha1.BlobGroupAnnotation]
		e.Group = groupAnnotation // Backward compatibility
		e.Groups = v1alpha1.ParseGroups(groupAnnotation)
	}

	// Set spec fields
	e.Priority = blob.Spec.Priority
	e.Total = blob.Status.Total
	e.ChunksNumber = blob.Spec.ChunksNumber

	// Set status fields
	e.Phase = string(blob.Status.Phase)
	e.Progress = blob.Status.Progress
	e.PendingChunks = blob.Status.PendingChunks
	e.RunningChunks = blob.Status.RunningChunks
	e.SucceededChunks = blob.Status.SucceededChunks
	e.FailedChunks = blob.Status.FailedChunks

	if e.Phase == string(v1alpha1.BlobPhaseRunning) &&
		e.Progress == 0 &&
		e.RunningChunks == 0 &&
		e.FailedChunks == 0 &&
		e.SucceededChunks == 0 {
		e.Phase = string(v1alpha1.BlobPhasePending)
	}

	if len(blob.Status.Conditions) > 0 {
		e.Errors = make([]string, 0, len(blob.Status.Conditions))
		for _, condition := range blob.Status.Conditions {
			e.Errors = append(e.Errors, fmt.Sprintf("%s: %s", condition.Type, condition.Message))
		}
	}

	return e
}

// chunkToEntry converts a Chunk to an entry for the WebUI
func chunkToEntry(chunk *v1alpha1.Chunk) *entry {
	e := &entry{}

	// Set metadata
	e.Name = chunk.Name
	e.DisplayName = chunk.Name
	if chunk.Annotations != nil {
		if dn := chunk.Annotations[v1alpha1.ChunkDisplayNameAnnotation]; dn != "" {
			e.DisplayName = dn
		}
		groupAnnotation := chunk.Annotations[v1alpha1.ChunkGroupAnnotation]
		e.Group = groupAnnotation // Backward compatibility
		e.Groups = v1alpha1.ParseGroups(groupAnnotation)

		if ignoreSize := chunk.Annotations[v1alpha1.ChunkGroupIgnoreSizeAnnotation]; ignoreSize == "true" {
			e.groupIgnoreSize = true
		}
	}

	// Set spec fields
	e.Priority = chunk.Spec.Priority
	e.Total = chunk.Spec.Total
	e.Progress = chunk.Status.Progress

	// Set status fields
	e.Phase = string(chunk.Status.Phase)
	e.Progress = chunk.Status.Progress

	if len(chunk.Status.Conditions) > 0 {
		e.Errors = make([]string, 0, len(chunk.Status.Conditions))
		for _, condition := range chunk.Status.Conditions {
			e.Errors = append(e.Errors, fmt.Sprintf("%s: %s", condition.Type, condition.Message))
		}
	}

	return e
}

// aggregateEntries aggregates multiple entries into a single group entry
func aggregateEntries(groupName string, entries map[string]*entry) *entry {
	aggregate := &entry{
		Name:        groupName,
		DisplayName: groupName,
		Members:     make([]entryMemberInfo, 0, len(entries)),
	}

	var totalSize int64
	var totalProgress int64
	var totalChunks int64
	var pendingChunks int64
	var runningChunks int64
	var succeededChunks int64
	var failedChunks int64
	var idleChunks int64
	var maxPriority int64
	var hasFailed bool

	for uid, e := range entries {
		// Add member info to the list
		aggregate.Members = append(aggregate.Members, entryMemberInfo{
			UID:         uid,
			DisplayName: e.DisplayName,
			Phase:       e.Phase,
		})

		if e.Phase == "Failed" {
			hasFailed = true
		}

		if e.Priority > maxPriority {
			maxPriority = e.Priority
		}

		if e.groupIgnoreSize {
			continue
		}

		totalSize += e.Total
		totalProgress += e.Progress
		totalChunks += e.ChunksNumber
		pendingChunks += e.PendingChunks
		runningChunks += e.RunningChunks
		succeededChunks += e.SucceededChunks
		failedChunks += e.FailedChunks
		idleChunks += e.ChunksNumber - (e.PendingChunks + e.RunningChunks + e.SucceededChunks + e.FailedChunks)

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

	case (hasFailed || failedChunks > 0) && pendingChunks == 0 && runningChunks == 0 && idleChunks == 0:
		aggregate.Phase = "Failed"
	case succeededChunks == totalChunks && totalChunks > 0:
		aggregate.Phase = "Succeeded"
	case totalSize > 0:
		aggregate.Phase = "Running"
	default:
		aggregate.Phase = "Pending"
	}

	return aggregate
}
