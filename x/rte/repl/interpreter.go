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

package repl

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/rte/rlimit"
	"strings"
	"time"
)

const (
	CONTEXT_DEADLINE_EXCEEDED = "context deadline exceeded"
)

type Input struct {
	AccountId    string                 `json:"accountId,omitempty"`
	Id           string                 `json:"id,omitempty"`
	InvocationId string                 `json:"invocationId,omitempty"`
	Runtime      string                 `json:"runtime,omitempty"`
	MainFn       string                 `json:"mainfn,omitempty"`
	Code         string                 `json:"code,omitempty"`
	Timeout      int                    `json:"timeout,omitempty"`
	Callback     string                 `json:"callback,omitempty"`
	Memory       uint64                 `json:"memory,omitempty"`
	Args         map[string]interface{} `json:"args,omitempty"`
}

type Output struct {
	StartedOn   time.Time     `json:"startedOn,omitempty"`
	FinishedOn  time.Time     `json:"finishedOn,omitempty"`
	ElapsedTime time.Duration `json:"elapsedTime,omitempty"`
	Stdout      string        `json:"stdout,omitempty"`
	Result      string        `json:"result,omitempty"`
	Status      string        `json:"status,omitempty"`
	ErrorDescr  string        `json:"errorDescr,omitempty"`
}

type Interpreter interface {
	DoREPL(input *Input) *Output
	Terminate()
}

func GetExecutionError(exec error, rErr error) *Error {
	mlog.Debug("Execution error: %v, rErr: %v", exec, rErr)

	if rErr != nil {
		if strings.Contains(rErr.Error(), rlimit.ERR_OUT_OF_MEMORY) {
			return NewError(SNIPPET_OUT_OF_MEMORY, SNIPPET_OUT_OF_MEMORY_DESCR)
		}
	}

	if strings.Contains(exec.Error(), CONTEXT_DEADLINE_EXCEEDED) {
		return NewError(SNIPPET_RUN_TIMEDOUT, SNIPPET_RUN_TIMEDOUT_DESCR)
	}

	return NewError(SNIPPET_REPL_FAILED, exec.Error())
}
