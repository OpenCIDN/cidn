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
			name:     "valid auth with password",
			auth:     "Basic dXNlcjpwYXNzd29yZA==", // user:password
			wantUser: "user",
			wantPass: "password",
			wantNil:  false,
		},
		{
			name:     "valid auth without password",
			auth:     "Basic dXNlcg==", // user
			wantUser: "user",
			wantPass: "",
			wantNil:  false,
		},
		{
			name:    "invalid prefix",
			auth:    "Bearer token123",
			wantNil: true,
		},
		{
			name:    "missing prefix",
			auth:    "dXNlcjpwYXNzd29yZA==",
			wantNil: true,
		},
		{
			name:    "invalid base64",
			auth:    "Basic invalid!!!",
			wantNil: true,
		},
		{
			name:    "empty string",
			auth:    "",
			wantNil: true,
		},
		{
			name:    "case insensitive prefix",
			auth:    "basic dXNlcjpwYXNzd29yZA==", // lowercase basic
			wantUser: "user",
			wantPass: "password",
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
				t.Errorf("ParseBasicAuth() = nil, want non-nil")
				return
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
			name: "with password",
			ui:   url.UserPassword("user", "password"),
			want: "Basic dXNlcjpwYXNzd29yZA==",
		},
		{
			name: "without password",
			ui:   url.User("user"),
			want: "Basic dXNlcg==",
		},
		{
			name: "nil userinfo",
			ui:   nil,
			want: "",
		},
		{
			name: "special characters in username",
			ui:   url.UserPassword("admin@domain.com", "pass123"),
			want: "Basic YWRtaW4lNDBkb21haW4uY29tOnBhc3MxMjM=", // @ is URL-encoded to %40
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormathBasicAuth(tt.ui); got != tt.want {
				t.Errorf("FormathBasicAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseAndFormatBasicAuth_RoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "simple credentials",
			username: "user",
			password: "password",
		},
		{
			name:     "username with numbers",
			username: "admin123",
			password: "secure123",
		},
		{
			name:     "alphanumeric only",
			username: "testuser",
			password: "Passw0rd123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Format credentials
			ui := url.UserPassword(tt.username, tt.password)
			formatted := FormathBasicAuth(ui)

			// Parse them back
			parsed := ParseBasicAuth(formatted)
			if parsed == nil {
				t.Fatal("ParseBasicAuth() returned nil")
			}

			// For alphanumeric credentials without special chars,
			// the round trip should preserve the values exactly
			if parsed.Username() != tt.username {
				t.Errorf("Round trip username = %v, want %v", parsed.Username(), tt.username)
			}

			parsedPass, _ := parsed.Password()
			if parsedPass != tt.password {
				t.Errorf("Round trip password = %v, want %v", parsedPass, tt.password)
			}
		})
	}
}
