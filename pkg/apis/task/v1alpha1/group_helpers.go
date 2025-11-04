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

import "strings"

// GetGroups extracts all group names from annotations using the specified prefix.
// It looks for annotation keys that start with the prefix and extracts the group name
// from the suffix of each matching key.
func GetGroups(annotations map[string]string, prefix string) []string {
	if annotations == nil || prefix == "" {
		return nil
	}
	
	groups := make([]string, 0)
	for key := range annotations {
		if strings.HasPrefix(key, prefix) {
			groupName := strings.TrimPrefix(key, prefix)
			if groupName != "" {
				groups = append(groups, groupName)
			}
		}
	}
	
	return groups
}

// SetGroups sets group annotations for the given groups using the specified prefix.
// It removes all existing group annotations with the prefix and adds new ones.
func SetGroups(annotations map[string]string, prefix string, groups []string) {
	if annotations == nil || prefix == "" {
		return
	}
	
	// Remove all existing group annotations
	for key := range annotations {
		if strings.HasPrefix(key, prefix) {
			delete(annotations, key)
		}
	}
	
	// Add new group annotations
	for _, group := range groups {
		group = strings.TrimSpace(group)
		if group != "" {
			annotations[prefix+group] = ""
		}
	}
}

// AddGroup adds a single group annotation using the specified prefix.
func AddGroup(annotations map[string]string, prefix string, group string) {
	if annotations == nil || prefix == "" {
		return
	}
	
	group = strings.TrimSpace(group)
	if group != "" {
		annotations[prefix+group] = ""
	}
}

// RemoveGroup removes a single group annotation using the specified prefix.
func RemoveGroup(annotations map[string]string, prefix string, group string) {
	if annotations == nil || prefix == "" {
		return
	}
	
	group = strings.TrimSpace(group)
	if group != "" {
		delete(annotations, prefix+group)
	}
}
