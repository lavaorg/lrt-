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

package events

import (
	"time"
)

type RTEEvent struct {
	Event     string    `json:"event,omitempty"`
	AccountId string    `json:"accountId,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Data      []byte    `json:"data,omitempty"`
}

type SnippetStartEvent struct {
	InvocationId string                 `json:"invocationId,omitempty"`
	SnippetId    string                 `json:"snippetId,omitempty"`
	Runtime      string                 `json:"runtime,omitempty"`
	MainFn       string                 `json:"mainfn,omitempty"`
	URL          string                 `json:"url,omitempty"`
	Code         string                 `json:"code,omitempty"`
	Timeout      int                    `json:"timeout,omitempty"`
	Callback     string                 `json:"callback,omitempty"`
	Memory       uint64                 `json:"memory,omitempty"`
	Args         map[string]interface{} `json:"args,omitempty"`
}

type SnippetStopEvent struct {
	InvocationId string `json:"invocationId,omitempty"`
}

type SnippetOutputEvent struct {
	InvocationId     string        `json:"invocationId,omitempty"`
	SnippetId        string        `json:"snippetId,omitempty"`
	RTEId            string        `json:"rteId,omitempty"`
	Status           string        `json:"status,omitempty"`
	ErrorDescription string        `json:"errorDescription,omitempty"`
	StartedOn        time.Time     `json:"startedOn,omitempty"`
	FinishedOn       time.Time     `json:"finishedOn,omitempty"`
	ElapsedTime      time.Duration `json:"elapsedTime,omitempty"`
	Callback         string        `json:"callback,omitempty"`
}
