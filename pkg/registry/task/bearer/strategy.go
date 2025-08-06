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

package bearer

import (
	"context"
	"fmt"
	"strings"
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

// NewStrategy creates and returns a bearerStrategy instance
func NewStrategy(typer runtime.ObjectTyper) *bearerStrategy {
	return &bearerStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Bearer
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*v1alpha1.Bearer)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Bearer")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchBearer is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
func MatchBearer(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *v1alpha1.Bearer) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, false)
}

type bearerStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

func (*bearerStrategy) NamespaceScoped() bool {
	return false
}

func (*bearerStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (*bearerStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newBearer := obj.(*v1alpha1.Bearer)
	oldBearer := old.(*v1alpha1.Bearer)

	newBearer.Status = oldBearer.Status
}

func (*bearerStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (*bearerStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

func (*bearerStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (*bearerStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (*bearerStrategy) Canonicalize(obj runtime.Object) {
	bearer := obj.(*v1alpha1.Bearer)

	if bearer.Status.Phase == "" {
		bearer.Status.Phase = v1alpha1.BearerPhasePending
	}
}

func (*bearerStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {

	return nil
}

// WarningsOnUpdate returns warnings for the given update.
func (*bearerStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (*bearerStrategy) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
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
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the bearer"},
			{Name: "Handler", Type: "string", Description: "Handler for the bearer"},
			{Name: "Phase", Type: "string", Description: "Current phase of the bearer"},
			{Name: "Age", Type: "date", Description: "Creation timestamp of the bearer"},
		}
	}

	fn := func(obj runtime.Object) error {
		bearer, ok := obj.(*v1alpha1.Bearer)
		if !ok {
			return fmt.Errorf("expected *v1alpha1.Bearer, got %T", obj)
		}

		phase := string(bearer.Status.Phase)
		if bearer.Status.Phase == v1alpha1.BearerPhaseFailed {
			faileds := []string{}
			for _, condition := range bearer.Status.Conditions {
				faileds = append(faileds, condition.Type)
			}
			if len(faileds) > 0 {
				phase += "(" + strings.Join(faileds, ",") + ")"
			}
		}

		row := metav1.TableRow{
			Cells: []interface{}{
				bearer.Name,
				bearer.Status.HandlerName,
				phase,
				time.Since(bearer.CreationTimestamp.Time).Truncate(time.Second).String(),
			},
		}

		switch opt.IncludeObject {
		case metav1.IncludeMetadata, "":
			partial := &metav1.PartialObjectMetadata{
				ObjectMeta: bearer.ObjectMeta,
			}
			row.Object = runtime.RawExtension{Object: partial}
		case metav1.IncludeObject:
			row.Object = runtime.RawExtension{Object: bearer}
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

type bearerStatusStrategy struct {
	*bearerStrategy
}

// NewStatusStrategy creates a bearerStatusStrategy instance.
func NewStatusStrategy(strategy *bearerStrategy) *bearerStatusStrategy {
	return &bearerStatusStrategy{strategy}
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update of status.
func (*bearerStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newBearer := obj.(*v1alpha1.Bearer)
	oldBearer := old.(*v1alpha1.Bearer)

	// Status changes are not allowed to update spec
	newBearer.Spec = oldBearer.Spec
}
