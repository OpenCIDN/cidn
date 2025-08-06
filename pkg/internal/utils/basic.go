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

package utils

import (
	"encoding/base64"
	"net/url"
	"strings"
)

func ParseBasicAuth(auth string) *url.Userinfo {
	const prefix = "Basic "
	if len(auth) < len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return nil
	}
	c, err := base64.StdEncoding.DecodeString(auth[len(prefix):])
	if err != nil {
		return nil
	}
	cs := string(c)
	username, password, ok := strings.Cut(cs, ":")
	if !ok {
		return url.User(username)
	}
	return url.UserPassword(username, password)
}

func FormathBasicAuth(ui *url.Userinfo) string {
	if ui == nil {
		return ""
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(ui.String()))
}
