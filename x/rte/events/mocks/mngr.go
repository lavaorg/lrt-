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

package mocks

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/rte/events"
	"github.com/verizonlabs/northstar/pkg/rte/repl"
)

type MockSnippetManager struct{}

func (m MockSnippetManager) SnippetStart(accountId string,
	start *events.SnippetStartEvent) (string, error) {
	mlog.Debug("Sending start event: %v, %v", accountId, start)
	return "", nil
}
func (m MockSnippetManager) SnippetStop(accountId string,
	partition int,
	stop *events.SnippetStopEvent) error {
	mlog.Debug("Sending stop event: %v, %v", accountId, partition, stop)
	return nil
}

func (m MockSnippetManager) SnippetOutput(accountId string,
	start *events.SnippetStartEvent,
	output *repl.Output) error {
	mlog.Debug("Sending output event: %v, %v, %v", accountId, start, output)
	return nil
}

func (m MockSnippetManager) UpdateInvocation(accountId string,
	invocationId string,
	partition int32,
	status string) error {
	mlog.Debug("Sending update event: %v, %v, %v, %v", accountId, invocationId, partition, status)
	return nil
}
