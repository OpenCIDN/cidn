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

package multipart

import (
	"context"
	"fmt"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
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

// NewStrategy creates and returns a multipartStrategy instance
func NewStrategy(typer runtime.ObjectTyper) *multipartStrategy {
	return &multipartStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Multipart
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*v1alpha1.Multipart)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Multipart")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchMultipart is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
func MatchMultipart(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *v1alpha1.Multipart) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, false)
}

type multipartStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

func (*multipartStrategy) NamespaceScoped() bool {
	return false
}

func (*multipartStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (*multipartStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {

}

func (*multipartStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (*multipartStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (*multipartStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (*multipartStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (*multipartStrategy) Canonicalize(obj runtime.Object) {

}

func (*multipartStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnUpdate returns warnings for the given update.
func (*multipartStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (*multipartStrategy) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
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
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the multipart"},
			{Name: "Age", Type: "date", Description: "Creation timestamp of the multipart"},
		}
	}

	fn := func(obj runtime.Object) error {
		multipart, ok := obj.(*v1alpha1.Multipart)
		if !ok {
			return fmt.Errorf("expected *v1alpha1.Multipart, got %T", obj)
		}

		row := metav1.TableRow{
			Cells: []interface{}{
				multipart.Name,
				time.Since(multipart.CreationTimestamp.Time).Truncate(time.Second).String(),
			},
		}

		switch opt.IncludeObject {
		case metav1.IncludeMetadata, "":
			partial := &metav1.PartialObjectMetadata{
				ObjectMeta: multipart.ObjectMeta,
			}
			row.Object = runtime.RawExtension{Object: partial}
		case metav1.IncludeObject:
			row.Object = runtime.RawExtension{Object: multipart}
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
