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

package user

import (
	"context"
	"slices"

	"k8s.io/apiserver/pkg/authorization/authorizer"
)

const (
	RunnerGroup = "cidn:runner"
)

var (
	RunnerAuthorizer runnerAuthorizer
)

type runnerAuthorizer struct {
}

func (runnerAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	if !slices.Contains(a.GetUser().GetGroups(), RunnerGroup) {
		return authorizer.DecisionNoOpinion, "", nil
	}

	switch a.GetResource() {
	case "chunks", "bearers":
		switch a.GetSubresource() {
		case "":
			if a.IsReadOnly() {
				return authorizer.DecisionAllow, "", nil
			}
		case "status":
			return authorizer.DecisionAllow, "", nil
		}
	}

	return authorizer.DecisionNoOpinion, "", nil
}
