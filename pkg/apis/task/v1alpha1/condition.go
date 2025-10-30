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

package v1alpha1

// Condition contains details for one aspect of the current state of this API Resource.
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
type Condition struct {
	// Type of condition in CamelCase or in foo.example.com/CamelCase.
	// Many .condition.type values are consistent across resources like Available, but because arbitrary conditions can be
	// useful (see .node.status.conditions), the ability to deconflict is important.
	// The regex it matches is (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	Type string `json:"type"`

	// Message is a human readable message indicating details about the transition.
	// This may be an empty string.
	Message string `json:"message"`
}

// ConditionStatus is the status of a condition.
// +enum
type ConditionStatus string

const (
	// ConditionTrue means a resource is in the condition.
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse means a resource is not in the condition.
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown means kubernetes can't decide if a resource is in the condition or not.
	ConditionUnknown ConditionStatus = "Unknown"
)

// AppendConditions appends new conditions to existing ones, merging duplicates by type.
// If a condition with the same type already exists, it will be updated with the new condition.
func AppendConditions(existing []Condition, newConditions ...Condition) []Condition {
	for _, newCond := range newConditions {
		found := false
		for i, existingCond := range existing {
			if existingCond.Type == newCond.Type {
				existing[i] = newCond
				found = true
				break
			}
		}
		if !found {
			existing = append(existing, newCond)
		}
	}
	return existing
}

// GetCondition returns the condition with the given type if it exists in the conditions list.
// The second return value indicates whether the condition was found.
func GetCondition(conditions []Condition, conditionType string) (Condition, bool) {
	for _, cond := range conditions {
		if cond.Type == conditionType {
			return cond, true
		}
	}
	return Condition{}, false
}
