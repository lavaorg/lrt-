/*
Copyright (C) 2017 Verizon. All Rights Reserved.

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

package management

import (
	"fmt"
	"net/http"
	"os"
	"strings"
)

var (
	health *Health = NewHealth(http.StatusOK, "OK", "service running normally")
)

// get and set the current health value for this service
func GetHealth() *Health {
	return health
}

func SetHealth(h *Health) {
	if h == nil {
		health = NewHealth(http.StatusOK, "OK", "service running normally")
	} else {
		health = h
	}
}

// adapted from Dakota, "User Services Sprint 5",
type Health struct {
	// http status (e.g., http.StatusOK)
	HttpStatus int `json:"-"`

	// service name
	Name string `json:"name,omitempty"`

	// developer-defined health status (enumeration, e.g., "OK", "BAD", etc)
	Id string `json:"id" binding:"required"`

	// status definition
	Description string `json:"description,omitempty"`

	// status uri
	Uri string `json:"uri,omitempty"`
}

func NewHealth(status int, id, template string, args ...interface{}) *Health {
	path := strings.Split(os.Args[0], "/")
	return &Health{
		HttpStatus:  status,
		Name:        path[len(path)-1],
		Id:          id,
		Description: fmt.Sprintf(template, args...),
	}
}

func (this *Health) String() string {
	return fmt.Sprintf("%v: %v", this.Id, this.Description)
}
