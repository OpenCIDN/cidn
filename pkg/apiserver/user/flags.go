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
	"fmt"
	"strings"
)

type UsersValue struct {
	Users []UserValue
}

func (d *UsersValue) Type() string {
	return "users"
}

func (d *UsersValue) Set(s string) error {
	userValue := &UserValue{}
	if err := userValue.Set(s); err != nil {
		return err
	}
	d.Users = append(d.Users, *userValue)
	return nil
}

func (d *UsersValue) String() string {
	var userStrings []string
	for _, user := range d.Users {
		userStrings = append(userStrings, user.String())
	}
	return strings.Join(userStrings, ";")
}

type UserValue struct {
	Name     string
	Password string
	Groups   []string
}

func (d *UserValue) Type() string {
	return "user"
}
func (d *UserValue) Set(s string) error {
	groupParts := strings.Split(s, ",")
	if len(groupParts) < 1 {
		return fmt.Errorf("invalid format, expected at least one group")
	}
	d.Groups = groupParts[1:]

	userParts := strings.SplitN(groupParts[0], ":", 2)
	if len(userParts) < 2 {
		return fmt.Errorf("invalid format, expected at least username and password")
	}

	d.Name = userParts[0]
	d.Password = userParts[1]

	return nil
}

func (d *UserValue) String() string {
	return fmt.Sprintf("%s:%s,%s", d.Name, d.Password, strings.Join(d.Groups, ","))
}
