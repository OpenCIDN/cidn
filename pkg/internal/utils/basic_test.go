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
	"testing"
)

func TestParseBasicAuth(t *testing.T) {
	tests := []struct {
		name     string
		auth     string
		wantUser string
		wantPass string
		wantNil  bool
	}{
		{
			name:     "valid basic auth with password",
			auth:     "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass")),
			wantUser: "user",
			wantPass: "pass",
			wantNil:  false,
		},
		{
			name:     "valid basic auth without password",
			auth:     "Basic " + base64.StdEncoding.EncodeToString([]byte("user")),
			wantUser: "user",
			wantPass: "",
			wantNil:  false,
		},
		{
			name:    "invalid prefix",
			auth:    "Bearer token",
			wantNil: true,
		},
		{
			name:    "empty string",
			auth:    "",
			wantNil: true,
		},
		{
			name:    "only Basic prefix",
			auth:    "Basic",
			wantNil: true,
		},
		{
			name:    "invalid base64",
			auth:    "Basic !!!invalid!!!",
			wantNil: true,
		},
		{
			name:     "case insensitive prefix",
			auth:     "basic " + base64.StdEncoding.EncodeToString([]byte("user:pass")),
			wantUser: "user",
			wantPass: "pass",
			wantNil:  false,
		},
		{
			name:     "username with colon in password",
			auth:     "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass:word")),
			wantUser: "user",
			wantPass: "pass:word",
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseBasicAuth(tt.auth)
			if tt.wantNil {
				if got != nil {
					t.Errorf("ParseBasicAuth() = %v, want nil", got)
				}
				return
			}

			if got == nil {
				t.Fatalf("ParseBasicAuth() = nil, want non-nil")
			}

			if got.Username() != tt.wantUser {
				t.Errorf("ParseBasicAuth() username = %v, want %v", got.Username(), tt.wantUser)
			}

			gotPass, _ := got.Password()
			if gotPass != tt.wantPass {
				t.Errorf("ParseBasicAuth() password = %v, want %v", gotPass, tt.wantPass)
			}
		})
	}
}

func TestFormathBasicAuth(t *testing.T) {
	tests := []struct {
		name string
		ui   *url.Userinfo
		want string
	}{
		{
			name: "nil userinfo",
			ui:   nil,
			want: "",
		},
		{
			name: "username only",
			ui:   url.User("user"),
			want: "Basic " + base64.StdEncoding.EncodeToString([]byte("user")),
		},
		{
			name: "username and password",
			ui:   url.UserPassword("user", "pass"),
			want: "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass")),
		},
		{
			name: "empty username",
			ui:   url.User(""),
			want: "Basic " + base64.StdEncoding.EncodeToString([]byte("")),
		},
		{
			name: "special characters in credentials",
			ui:   url.UserPassword("admin@example.com", "p@ss:w0rd!"),
			want: "Basic " + base64.StdEncoding.EncodeToString([]byte("admin%40example.com:p%40ss%3Aw0rd%21")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormathBasicAuth(tt.ui)
			if got != tt.want {
				t.Errorf("FormathBasicAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoundTripBasicAuth(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{"simple", "user", "pass"},
		{"no password", "user", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := url.UserPassword(tt.username, tt.password)
			formatted := FormathBasicAuth(original)
			parsed := ParseBasicAuth(formatted)

			if parsed == nil {
				t.Fatalf("ParseBasicAuth() returned nil")
			}

			// Note: url.Userinfo.String() URL-encodes special characters,
			// so we compare against the URL-encoded version
			expectedUser := original.Username()
			if parsed.Username() != expectedUser {
				t.Errorf("username = %v, want %v", parsed.Username(), expectedUser)
			}

			expectedPass, _ := original.Password()
			gotPass, _ := parsed.Password()
			if gotPass != expectedPass {
				t.Errorf("password = %v, want %v", gotPass, expectedPass)
			}
		})
	}
}
