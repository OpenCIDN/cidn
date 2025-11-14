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
	"sort"
	"strings"
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
		ctx := r.Context()
		// Set headers for Server-Sent Events
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Channel for direct updates
		updates := make(chan Event, 4)
		defer close(updates)

		bufferUpdates := make(chan Event, 4)
		defer close(bufferUpdates)

		// Track group for aggregation
		groups := map[string]map[string]*entry{}
		groupMutex := &sync.RWMutex{}
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		// Helper function to delete a group aggregate

		addToUpdates := func(e Event) {
			select {
			case updates <- e:
			case <-ctx.Done():
			}
		}
		addToBufferUpdates := func(e Event) {
			select {
			case bufferUpdates <- e:
			case <-ctx.Done():
			}
		}

		createGroupEvent := func(group string, name string, e *entry) Event {
			groupMutex.Lock()
			defer groupMutex.Unlock()
			if groups[group] == nil {
				groups[group] = make(map[string]*entry)
			}

			if e != nil {
				groups[group][name] = e
			}

			aggregate := aggregateEntries(group, groups[group])
			event := Event{
				Type: "UPDATE",
				ID:   "group:" + group,
			}
			data, _ := json.Marshal(aggregate)
			event.Data = data
			return event
		}

		// Helper function to remove blob from group and update or delete group aggregate
		removeFromGroup := func(group string, name string) Event {
			groupMutex.Lock()
			defer groupMutex.Unlock()
			if _, exists := groups[group]; !exists {
				event := Event{Type: "DELETE", ID: "group:" + group}
				return event
			}

			delete(groups[group], name)
			if len(groups[group]) == 0 {
				event := Event{Type: "DELETE", ID: "group:" + group}
				return event
			}

			aggregate := aggregateEntries(group, groups[group])
			event := Event{
				Type: "UPDATE",
				ID:   "group:" + group,
			}
			data, _ := json.Marshal(aggregate)
			event.Data = data
			return event
		}

		resourceEventHandlerRegistration, err := blobInformerInstance.AddEventHandler(&cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if ctx.Err() != nil {
					return
				}
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				if blob.Annotations == nil {
					return
				}

				displayName := blob.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				e := blobToEntry(blob)
				event := createEvent("ADD", blob.Name, e)
				addToUpdates(event)

				// Track group membership
				if group := blob.Annotations[v1alpha1.WebuiGroupAnnotation]; group != "" {
					groupEvent := createGroupEvent(group, blob.Name, e)
					addToUpdates(groupEvent)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				if ctx.Err() != nil {
					return
				}
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

				displayName := newBlob.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				e := blobToEntry(newBlob)
				event := createEvent("UPDATE", newBlob.Name, e)

				needBuffer := oldBlob.Status.Phase == newBlob.Status.Phase && newBlob.Status.Progress != newBlob.Status.Total
				if needBuffer {
					addToBufferUpdates(event)
				} else {
					addToUpdates(event)
				}

				// Update group membership
				oldGroup := oldBlob.Annotations[v1alpha1.WebuiGroupAnnotation]
				newGroup := newBlob.Annotations[v1alpha1.WebuiGroupAnnotation]

				// Remove from old group if changed
				if oldGroup != "" && oldGroup != newGroup {
					removeFromGroup(oldGroup, newBlob.Name)
				}

				// Add to new group
				if newGroup != "" {
					groupEvent := createGroupEvent(newGroup, newBlob.Name, e)
					if needBuffer {
						addToBufferUpdates(groupEvent)
					} else {
						addToUpdates(groupEvent)
					}

				}
			},
			DeleteFunc: func(obj interface{}) {
				if ctx.Err() != nil {
					return
				}
				blob, ok := obj.(*v1alpha1.Blob)
				if !ok {
					return
				}

				if blob.Annotations == nil {
					return
				}

				displayName := blob.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				e := blobToEntry(blob)
				event := createEvent("DELETE", blob.Name, e)
				addToBufferUpdates(event)

				// Remove from group
				if group := blob.Annotations[v1alpha1.WebuiGroupAnnotation]; group != "" {
					groupEvent := removeFromGroup(group, blob.Name)
					addToBufferUpdates(groupEvent)
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
				if ctx.Err() != nil {
					return
				}
				chunk, ok := obj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				if chunk.Annotations == nil {
					return
				}

				displayName := chunk.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				e := chunkToEntry(chunk)
				event := createEvent("ADD", chunk.Name, e)
				addToUpdates(event)

				// Track group membership
				if group := chunk.Annotations[v1alpha1.WebuiGroupAnnotation]; group != "" {
					groupEvent := createGroupEvent(group, chunk.Name, e)
					addToUpdates(groupEvent)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				if ctx.Err() != nil {
					return
				}
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

				displayName := newChunk.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				e := chunkToEntry(newChunk)
				event := createEvent("UPDATE", newChunk.Name, e)
				needBuffer := oldChunk.Status.Phase == newChunk.Status.Phase && newChunk.Status.Progress != newChunk.Spec.Total
				if needBuffer {
					addToBufferUpdates(event)
				} else {
					addToUpdates(event)
				}

				// Update group membership
				oldGroup := oldChunk.Annotations[v1alpha1.WebuiGroupAnnotation]
				newGroup := newChunk.Annotations[v1alpha1.WebuiGroupAnnotation]

				// Remove from old group if changed
				if oldGroup != "" && oldGroup != newGroup {
					groupEvent := removeFromGroup(oldGroup, newChunk.Name)
					if needBuffer {
						addToBufferUpdates(groupEvent)
					} else {
						addToUpdates(groupEvent)
					}
				}

				// Add to new group
				if newGroup != "" {
					groupEvent := createGroupEvent(newGroup, newChunk.Name, e)
					if needBuffer {
						addToBufferUpdates(groupEvent)
					} else {
						addToUpdates(groupEvent)
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				if ctx.Err() != nil {
					return
				}
				chunk, ok := obj.(*v1alpha1.Chunk)
				if !ok {
					return
				}

				if chunk.Annotations == nil {
					return
				}

				displayName := chunk.Annotations[v1alpha1.WebuiDisplayNameAnnotation]
				if displayName == "" {
					return
				}

				e := chunkToEntry(chunk)
				event := createEvent("DELETE", chunk.Name, e)
				addToUpdates(event)

				// Remove from group
				if group := chunk.Annotations[v1alpha1.WebuiGroupAnnotation]; group != "" {
					groupEvent := removeFromGroup(group, chunk.Name)
					addToBufferUpdates(groupEvent)
				}
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to add chunk event handler: %v", err), http.StatusInternalServerError)
			return
		}
		defer chunkInformerInstance.RemoveEventHandler(chunkEventHandlerRegistration)

		bufferUpdateMap := map[string]*Event{}
		// Stream updates to client
		flusher := w.(http.Flusher)
		for {
			select {
			case <-ticker.C:
				if len(bufferUpdateMap) != 0 {
					for _, event := range bufferUpdateMap {
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
					clear(bufferUpdateMap)
				}
			case event := <-bufferUpdates:
				bufferUpdateMap[event.ID] = &event
			case event := <-updates:
				bufferUpdateMap[event.ID] = nil
				_, err := event.WriteTo(w)
				if err != nil {
					fmt.Printf("Error writing event: %v\n", err)
					return
				}
				flusher.Flush()
			case <-ctx.Done():
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
	ID    string `json:"id"`
	Name  string `json:"name"`
	Phase string `json:"phase"`
}

// entry is a unified view of Blob or Chunk for the WebUI
// Only relevant fields are exposed
type entry struct {
	Name    string            `json:"name,omitempty"`
	Group   string            `json:"group,omitempty"`
	Tags    []string          `json:"tags,omitempty"`
	Members []entryMemberInfo `json:"members,omitempty"` // For group aggregates: list of member info

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

// parseTagsFromAnnotation parses a comma-separated list of tags from an annotation value
func parseTagsFromAnnotation(tagsStr string) []string {
	if tagsStr == "" {
		return nil
	}
	tags := strings.Split(tagsStr, ",")
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		if trimmed := strings.TrimSpace(tag); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return nil
	}

	sort.Strings(result)
	return result
}

// blobToEntry converts a Blob to an entry for the WebUI
func blobToEntry(blob *v1alpha1.Blob) *entry {
	e := &entry{}

	// Set metadata
	if blob.Annotations != nil {
		if dn := blob.Annotations[v1alpha1.WebuiDisplayNameAnnotation]; dn != "" {
			e.Name = dn
		}
		e.Group = blob.Annotations[v1alpha1.WebuiGroupAnnotation]
		e.Tags = parseTagsFromAnnotation(blob.Annotations[v1alpha1.WebuiTagAnnotation])
	}

	if e.Name == "" {
		e.Name = blob.Name
	}

	e.Progress = blob.Status.Progress
	e.ChunksNumber = blob.Spec.ChunksNumber
	e.Phase = string(blob.Status.Phase)
	e.PendingChunks = blob.Status.PendingChunks
	e.RunningChunks = blob.Status.RunningChunks
	e.SucceededChunks = blob.Status.SucceededChunks
	e.FailedChunks = blob.Status.FailedChunks

	if blob.Status.Total > 0 {
		e.Total = blob.Status.Total
	}

	if e.Progress == 0 &&
		e.RunningChunks == 0 &&
		e.FailedChunks == 0 &&
		e.SucceededChunks == 0 {
		if e.Phase == string(v1alpha1.BlobPhaseRunning) {
			e.Phase = string(v1alpha1.BlobPhasePending)
		}
		if e.Phase == string(v1alpha1.BearerPhaseFailed) {
			e.ChunksNumber = 0
		}
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
	if chunk.Annotations != nil {
		if dn := chunk.Annotations[v1alpha1.WebuiDisplayNameAnnotation]; dn != "" {
			e.Name = dn
		}
		e.Group = chunk.Annotations[v1alpha1.WebuiGroupAnnotation]

		if ignoreSize := chunk.Annotations[v1alpha1.WebuiGroupIgnoreSizeAnnotation]; ignoreSize == "true" {
			e.groupIgnoreSize = true
		}

		e.Tags = parseTagsFromAnnotation(chunk.Annotations[v1alpha1.WebuiTagAnnotation])
	}
	if e.Name == "" {
		e.Name = chunk.Name
	}

	e.Progress = chunk.Status.Progress
	if chunk.Spec.Total > 0 {
		e.Total = chunk.Spec.Total
	}
	if chunk.Status.Phase == v1alpha1.ChunkPhaseFailed && chunk.Status.Retryable {
		e.Phase = string(v1alpha1.ChunkPhaseRunning)
	} else {
		e.Phase = string(chunk.Status.Phase)
		if len(chunk.Status.Conditions) > 0 {
			e.Errors = make([]string, 0, len(chunk.Status.Conditions))
			for _, condition := range chunk.Status.Conditions {
				e.Errors = append(e.Errors, fmt.Sprintf("%s: %s", condition.Type, condition.Message))
			}
		}
	}
	return e
}

// aggregateEntries aggregates multiple entries into a single group entry
func aggregateEntries(groupName string, entries map[string]*entry) *entry {
	aggregate := &entry{
		Name:    groupName,
		Members: make([]entryMemberInfo, 0, len(entries)),
	}

	var totalProgress int64
	var totalChunks int64
	var pendingChunks int64
	var runningChunks int64
	var succeededChunks int64
	var failedChunks int64
	var idleChunks int64
	var hasFailed bool
	var hasRunning bool
	var hasPending bool

	for uid, e := range entries {
		aggregate.Members = append(aggregate.Members, entryMemberInfo{
			ID:    uid,
			Name:  e.Name,
			Phase: e.Phase,
		})

		switch e.Phase {
		case "Failed":
			hasFailed = true
		case "Running":
			hasRunning = true
		case "Pending":
			hasPending = true
		}

		if e.groupIgnoreSize {
			continue
		}

		totalProgress += e.Progress
		totalChunks += e.ChunksNumber
		pendingChunks += e.PendingChunks
		runningChunks += e.RunningChunks
		succeededChunks += e.SucceededChunks
		failedChunks += e.FailedChunks
		idleChunks += e.ChunksNumber - (e.PendingChunks + e.RunningChunks + e.SucceededChunks + e.FailedChunks)
	}

	sort.Slice(aggregate.Members, func(i, j int) bool {
		return aggregate.Members[i].Name < aggregate.Members[j].Name
	})

	aggregate.Progress = totalProgress
	aggregate.ChunksNumber = totalChunks
	aggregate.PendingChunks = pendingChunks
	aggregate.RunningChunks = runningChunks
	aggregate.SucceededChunks = succeededChunks
	aggregate.FailedChunks = failedChunks

	completed := pendingChunks == 0 && runningChunks == 0 && idleChunks == 0 && !hasRunning && !hasPending

	switch {
	case completed && (hasFailed || failedChunks > 0):
		aggregate.Phase = "Failed"
	case completed:
		aggregate.Phase = "Succeeded"
	case !hasRunning && hasPending:
		aggregate.Phase = "Pending"
	default:
		aggregate.Phase = "Running"
	}

	return aggregate
}
