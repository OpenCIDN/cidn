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

package blob

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

// NewStrategy creates and returns a blobStrategy instance
func NewStrategy(typer runtime.ObjectTyper) *blobStrategy {
	return &blobStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Blob
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*v1alpha1.Blob)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Blob")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchBlob is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
func MatchBlob(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *v1alpha1.Blob) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

type blobStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

func (*blobStrategy) NamespaceScoped() bool {
	return false
}

func (*blobStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (*blobStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

func (*blobStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	blob := obj.(*v1alpha1.Blob)

	var errList field.ErrorList

	if len(blob.Spec.Source) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "source"), "at least one source must be specified"))
	}

	if len(blob.Spec.Destination) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "destination"), "at least one destination must be specified"))
	}

	return errList
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (*blobStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

func (*blobStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (*blobStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (*blobStrategy) Canonicalize(obj runtime.Object) {
	blob := obj.(*v1alpha1.Blob)

	if blob.Status.Phase == "" {
		blob.Status.Phase = v1alpha1.BlobPhasePending
	}

	if blob.Spec.Total != 0 {
		if blob.Spec.MinimumChunkSize != 0 {
			blob.Spec.ChunksNumber = getChunksNumberByMinimumChunkSize(blob.Spec.Total, blob.Spec.MinimumChunkSize)
			blob.Spec.ChunkSize = getChunkSize(blob.Spec.Total, blob.Spec.ChunksNumber)
		} else if blob.Spec.ChunkSize == 0 && blob.Spec.ChunksNumber != 0 {
			blob.Spec.ChunkSize = getChunkSize(blob.Spec.Total, blob.Spec.ChunksNumber)
		} else if blob.Spec.ChunkSize != 0 && blob.Spec.ChunksNumber == 0 {
			blob.Spec.ChunksNumber = getChunksNumber(blob.Spec.Total, blob.Spec.ChunkSize)
		} else {
			blob.Spec.ChunksNumber = 1
			blob.Spec.ChunkSize = blob.Spec.Total
		}
	}
}

func getChunksNumberByMinimumChunkSize(total, minimumChunkSize int64) int64 {
	chunksNumber := total / minimumChunkSize
	if chunksNumber <= 1 {
		return 1
	}
	if chunksNumber >= 10000 {
		return 10000
	}
	return chunksNumber
}

func getChunkSize(total, chunksNumber int64) int64 {
	if total <= 5*1024*1024 { // <= 5MiB
		return total
	}
	chunkSize := total / chunksNumber
	if total%chunksNumber != 0 {
		chunkSize++
	}

	if chunkSize >= 5*1024*1024*1024 { // <= 5GiB
		return 5 * 1024 * 1024 * 1024
	}
	return chunkSize
}

func getChunksNumber(total, chunkSize int64) int64 {
	if chunkSize == 0 {
		return 1
	}
	if chunkSize >= total {
		return 1
	}

	count := total / chunkSize
	if total%chunkSize != 0 {
		count++
	}
	return count
}

func (*blobStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newBlob := obj.(*v1alpha1.Blob)
	oldBlob := old.(*v1alpha1.Blob)
	var errList field.ErrorList

	if len(newBlob.Spec.Source) < len(oldBlob.Spec.Source) {
		errList = append(errList, field.Forbidden(field.NewPath("spec", "source"), "cannot reduce number of sources"))
	}

	for i := range oldBlob.Spec.Source {
		if i >= len(newBlob.Spec.Source) {
			break
		}
		if newBlob.Spec.Source[i].URL != oldBlob.Spec.Source[i].URL {
			errList = append(errList, field.Forbidden(field.NewPath("spec", "source").Index(i).Child("url"), "source URL is immutable"))
		}
	}

	if !reflect.DeepEqual(newBlob.Spec.Destination, oldBlob.Spec.Destination) {
		errList = append(errList, field.Forbidden(field.NewPath("spec", "destination"), "destination is immutable"))
	}

	return errList
}

// WarningsOnUpdate returns warnings for the given update.
func (*blobStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (*blobStrategy) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
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
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the blob"},
			{Name: "Handler", Type: "string", Description: "Handler for the blob"},
			{Name: "Phase", Type: "string", Description: "Current phase of the blob"},
			{Name: "Progress", Type: "string", Description: "Progress of the blob"},
			{Name: "Chunks", Type: "string", Description: "Completed chunks of the blob"},
			{Name: "Age", Type: "date", Description: "Creation timestamp of the blob"},
		}
	}

	fn := func(obj runtime.Object) error {
		blob, ok := obj.(*v1alpha1.Blob)
		if !ok {
			return fmt.Errorf("expected *v1alpha1.Blob, got %T", obj)
		}

		phase := string(blob.Status.Phase)
		if blob.Status.Phase == v1alpha1.BlobPhaseFailed {
			faileds := []string{}
			for _, condition := range blob.Status.Conditions {
				faileds = append(faileds, condition.Type)
			}
			if len(faileds) > 0 {
				phase += "(" + strings.Join(faileds, ",") + ")"
			}
		}

		progress := "<none>"
		if blob.Spec.Total != 0 {
			if blob.Status.Progress == blob.Spec.Total {
				progress = humanize.IBytes(uint64(blob.Spec.Total))
			} else {
				progress = fmt.Sprintf("%s/%s", humanize.IBytes(uint64(blob.Status.Progress)), humanize.IBytes(uint64(blob.Spec.Total)))
			}
		}

		chunks := "<none>"
		if blob.Spec.ChunksNumber > 1 {
			chunks = fmt.Sprintf("%d/%d", blob.Status.SucceededChunks, blob.Spec.ChunksNumber)
		}

		row := metav1.TableRow{
			Cells: []interface{}{
				blob.Name,
				blob.Spec.HandlerName,
				phase,
				progress,
				chunks,
				time.Since(blob.CreationTimestamp.Time).Truncate(time.Second).String(),
			},
		}

		switch opt.IncludeObject {
		case metav1.IncludeMetadata, "":
			partial := &metav1.PartialObjectMetadata{
				ObjectMeta: blob.ObjectMeta,
			}
			row.Object = runtime.RawExtension{Object: partial}
		case metav1.IncludeObject:
			row.Object = runtime.RawExtension{Object: blob}
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
