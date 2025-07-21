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

package sync

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

// NewStrategy creates and returns a syncStrategy instance
func NewStrategy(typer runtime.ObjectTyper) *syncStrategy {
	return &syncStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Sync
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*v1alpha1.Sync)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Sync")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchSync is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
func MatchSync(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *v1alpha1.Sync) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

type syncStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

func (*syncStrategy) NamespaceScoped() bool {
	return false
}

func (*syncStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (*syncStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

func (*syncStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	sync := obj.(*v1alpha1.Sync)
	var errList field.ErrorList

	if sync.Spec.Total <= 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "total"), "total must be greater than 0"))
	}

	if len(sync.Spec.Source.Request.URL) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "source", "request", "url"), "source URL must be specified"))
	}

	if len(sync.Spec.Destination) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "destination"), "at least one destination must be specified"))
	}

	return errList
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (*syncStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

func (*syncStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (*syncStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (*syncStrategy) Canonicalize(obj runtime.Object) {
	sync := obj.(*v1alpha1.Sync)

	if sync.Status.Phase == "" {
		sync.Status.Phase = v1alpha1.SyncPhasePending
	}
}

func (*syncStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newSync := obj.(*v1alpha1.Sync)
	oldSync := old.(*v1alpha1.Sync)
	var errList field.ErrorList

	if newSync.Spec.Total != oldSync.Spec.Total {
		errList = append(errList, field.Forbidden(field.NewPath("spec", "total"), "total is immutable"))
	}

	if newSync.Spec.Source.Request.URL != oldSync.Spec.Source.Request.URL {
		errList = append(errList, field.Forbidden(field.NewPath("spec", "source", "request", "url"), "source URL is immutable"))
	}

	if !reflect.DeepEqual(newSync.Spec.Destination, oldSync.Spec.Destination) {
		errList = append(errList, field.Forbidden(field.NewPath("spec", "destination"), "destination is immutable"))
	}

	return errList
}

// WarningsOnUpdate returns warnings for the given update.
func (*syncStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (*syncStrategy) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
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
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the sync"},
			{Name: "Handler", Type: "string", Description: "Handler for the sync"},
			{Name: "Phase", Type: "string", Description: "Current phase of the sync"},
			{Name: "Progress", Type: "string", Description: "Progress of the sync"},
			{Name: "Age", Type: "date", Description: "Creation timestamp of the sync"},
		}
	}

	fn := func(obj runtime.Object) error {
		sync, ok := obj.(*v1alpha1.Sync)
		if !ok {
			return fmt.Errorf("expected *v1alpha1.Sync, got %T", obj)
		}

		phase := string(sync.Status.Phase)
		if sync.Status.Phase == v1alpha1.SyncPhaseFailed {
			faileds := []string{}
			for _, condition := range sync.Status.Conditions {
				faileds = append(faileds, condition.Type)
			}
			if len(faileds) > 0 {
				phase += "(" + strings.Join(faileds, ",") + ")"
			}
		}

		var progress string
		if sync.Status.Progress == sync.Spec.Total {
			progress = humanize.IBytes(uint64(sync.Spec.Total))
		} else {
			progress = fmt.Sprintf("%s/%s", humanize.IBytes(uint64(sync.Status.Progress)), humanize.IBytes(uint64(sync.Spec.Total)))
		}

		row := metav1.TableRow{
			Cells: []interface{}{
				sync.Name,
				sync.Spec.HandlerName,
				phase,
				progress,
				time.Since(sync.CreationTimestamp.Time).Truncate(time.Second).String(),
			},
		}

		switch opt.IncludeObject {
		case metav1.IncludeMetadata, "":
			partial := &metav1.PartialObjectMetadata{
				ObjectMeta: sync.ObjectMeta,
			}
			row.Object = runtime.RawExtension{Object: partial}
		case metav1.IncludeObject:
			row.Object = runtime.RawExtension{Object: sync}
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
