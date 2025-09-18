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

package chunk

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	humanize "github.com/dustin/go-humanize"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
)

// NewStrategy creates and returns a chunkStrategy instance
func NewStrategy(typer runtime.ObjectTyper) *chunkStrategy {
	return &chunkStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Chunk
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*v1alpha1.Chunk)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Chunk")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchChunk is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
func MatchChunk(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *v1alpha1.Chunk) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, false)
}

type chunkStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

func (*chunkStrategy) NamespaceScoped() bool {
	return false
}

func (*chunkStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (*chunkStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newChunk := obj.(*v1alpha1.Chunk)
	oldChunk := old.(*v1alpha1.Chunk)

	newChunk.Status = oldChunk.Status
}

func (*chunkStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	chunk := obj.(*v1alpha1.Chunk)
	var errList field.ErrorList

	if len(chunk.Spec.Source.Request.URL) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "source", "request", "url"), "source URL must be specified"))
	}

	return errList
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (*chunkStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

func (*chunkStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (*chunkStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (*chunkStrategy) Canonicalize(obj runtime.Object) {
	chunk := obj.(*v1alpha1.Chunk)

	if chunk.Status.Phase == "" {
		chunk.Status.Phase = v1alpha1.ChunkPhasePending
	}
}

func (*chunkStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newChunk := obj.(*v1alpha1.Chunk)
	oldChunk := old.(*v1alpha1.Chunk)
	var errList field.ErrorList

	if !reflect.DeepEqual(newChunk.Spec, oldChunk.Spec) {
		errList = append(errList, field.Forbidden(field.NewPath("spec"), "spec is immutable"))
	}

	return errList
}

// WarningsOnUpdate returns warnings for the given update.
func (*chunkStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (*chunkStrategy) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	table := &metav1.Table{}

	if m, err := meta.ListAccessor(object); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else {
		if m, err := meta.CommonAccessor(object); err == nil {
			table.ResourceVersion = m.GetResourceVersion()
		}
	}

	opt, ok := tableOptions.(*metav1.TableOptions)
	if !ok {
		opt = &metav1.TableOptions{}
	}
	if !opt.NoHeaders {
		table.ColumnDefinitions = []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the chunk"},
			{Name: "Handler", Type: "string", Description: "Handler for the chunk"},
			{Name: "Phase", Type: "string", Description: "Current phase of the chunk"},
			{Name: "Progress", Type: "string", Description: "Progress of the chunk"},
			{Name: "Age", Type: "date", Description: "Creation timestamp of the chunk"},
		}
	}

	fn := func(obj runtime.Object) error {
		chunk, ok := obj.(*v1alpha1.Chunk)
		if !ok {
			return fmt.Errorf("expected *v1alpha1.Chunk, got %T", obj)
		}

		phase := string(chunk.Status.Phase)
		if chunk.Status.Phase == v1alpha1.ChunkPhaseFailed {
			faileds := []string{}
			for _, condition := range chunk.Status.Conditions {
				faileds = append(faileds, condition.Type)
			}
			if len(faileds) > 0 {
				phase += "(" + strings.Join(faileds, ",") + ")"
			}
		}

		progress := "<none>"
		if chunk.Spec.Total != 0 {
			if chunk.Status.Progress == chunk.Spec.Total {
				progress = humanize.IBytes(uint64(chunk.Spec.Total))
			} else if chunk.Spec.Total > 0 {
				progress = fmt.Sprintf("%s/%s", humanize.IBytes(uint64(chunk.Status.Progress)), humanize.IBytes(uint64(chunk.Spec.Total)))
			} else {
				progress = fmt.Sprintf("%s/<unknow>", humanize.IBytes(uint64(chunk.Status.Progress)))
			}
		}

		row := metav1.TableRow{
			Cells: []interface{}{
				chunk.Name,
				chunk.Status.HandlerName,
				phase,
				progress,
				time.Since(chunk.CreationTimestamp.Time).Truncate(time.Second).String(),
			},
		}

		switch opt.IncludeObject {
		case metav1.IncludeMetadata, "":
			partial := &metav1.PartialObjectMetadata{
				ObjectMeta: chunk.ObjectMeta,
			}
			row.Object = runtime.RawExtension{Object: partial}
		case metav1.IncludeObject:
			row.Object = runtime.RawExtension{Object: chunk}
		}

		table.Rows = append(table.Rows, row)
		return nil
	}
	switch {
	case meta.IsListType(object):
		if err := meta.EachListItem(object, fn); err != nil {
			return nil, err
		}
	default:
		if err := fn(object); err != nil {
			return nil, err
		}
	}

	return table, nil
}

// chunkStatusStrategy implements the logic for chunk status updates.
type chunkStatusStrategy struct {
	*chunkStrategy
}

// NewStatusStrategy creates a chunkStatusStrategy instance.
func NewStatusStrategy(strategy *chunkStrategy) *chunkStatusStrategy {
	return &chunkStatusStrategy{strategy}
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update of status.
func (*chunkStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newChunk := obj.(*v1alpha1.Chunk)
	oldChunk := old.(*v1alpha1.Chunk)

	// Status changes are not allowed to update spec
	newChunk.Spec = oldChunk.Spec
}
