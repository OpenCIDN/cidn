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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
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

	if blob.Spec.Source == "" {
		errList = append(errList, field.Required(field.NewPath("spec", "source"), "source must be specified"))
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
}

func (*blobStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newBlob := obj.(*v1alpha1.Blob)
	oldBlob := old.(*v1alpha1.Blob)
	var errList field.ErrorList

	if newBlob.Spec.Source != oldBlob.Spec.Source {
		errList = append(errList, field.Forbidden(field.NewPath("spec", "source"), "source is immutable"))
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
	if opt, ok := tableOptions.(*metav1.TableOptions); !ok || !opt.NoHeaders {
		table.ColumnDefinitions = []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the blob"},
			{Name: "Handler", Type: "string", Description: "Handler for the blob"},
			{Name: "Source", Type: "string", Description: "Source of the blob"},
			{Name: "Phase", Type: "string", Description: "Current phase of the blob"},
			{Name: "Progress", Type: "string", Description: "Progress of the blob"},
			{Name: "Created At", Type: "date", Description: "Creation timestamp of the blob"},
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
				if condition.Status == v1alpha1.ConditionTrue && condition.Reason != "" {
					faileds = append(faileds, condition.Reason)
				}
			}
			if len(faileds) > 0 {
				phase += "(" + strings.Join(faileds, ",") + ")"
			}
		}
		table.Rows = append(table.Rows, metav1.TableRow{
			Cells: []interface{}{
				blob.Name,
				blob.Spec.HandlerName,
				blob.Spec.Source,
				phase,
				fmt.Sprintf("%d/%d", blob.Status.Progress, blob.Spec.Total),
				blob.CreationTimestamp.Time.UTC().Format(time.RFC3339),
			},
			Object: runtime.RawExtension{Object: blob},
		})
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
