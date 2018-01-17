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
	"github.com/orcaman/concurrent-map"
	"os"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/kafka"
	"github.com/verizonlabs/northstar/pkg/rte/repl"
	"github.com/verizonlabs/northstar/pkg/rte/util"
	"time"
)

type SnippetRunWorker struct {
	accountId      string
	stopOffset     int64
	workers        cmap.ConcurrentMap
	interpreter    repl.Interpreter
	snippetManager SnippetManager
	startEvent     *SnippetStartEvent
	processMsg     *kafka.ProcessMsg
}

func NewSnippetRunWorker(accountId string,
	workers cmap.ConcurrentMap,
	snippetManager SnippetManager,
	startEvent *SnippetStartEvent,
	interpreter repl.Interpreter,
	processMsg *kafka.ProcessMsg) *SnippetRunWorker {
	mlog.Debug("NewSnippetRunWorker")
	return &SnippetRunWorker{accountId: accountId,
		workers:        workers,
		interpreter:    interpreter,
		snippetManager: snippetManager,
		startEvent:     startEvent,
		processMsg:     processMsg,
	}
}

func (worker *SnippetRunWorker) Run(workRoutine int) error {
	mlog.Debug("Running snippet run worker: %v", workRoutine)

	err := worker.snippetManager.UpdateInvocation(worker.accountId,
		worker.startEvent.InvocationId,
		worker.processMsg.Event.Partition,
		SNIPPET_RUNNING_EVENT)
	if err != nil {
		mlog.Error("UpdateInvocation failed: %v", err)
		return err
	}

	code, err := util.GetSnippetCode(worker.startEvent.URL, worker.startEvent.Code)
	if err != nil {
		mlog.Error("Failed to get snippet: %v", err.Error())
		return err
	}

	runSnippet := repl.Input{AccountId: worker.accountId,
		Id:           worker.startEvent.SnippetId,
		InvocationId: worker.startEvent.InvocationId,
		Runtime:      worker.startEvent.Runtime,
		MainFn:       worker.startEvent.MainFn,
		Code:         code,
		Timeout:      worker.startEvent.Timeout,
		Callback:     worker.startEvent.Callback,
		Memory:       worker.startEvent.Memory,
		Args:         worker.startEvent.Args}

	output := worker.interpreter.DoREPL(&runSnippet)
	err = worker.snippetManager.SnippetOutput(worker.accountId, worker.startEvent, output)
	if err != nil {
		mlog.Error("Failed to process snippet output: %v", err)
		return err
	}

	mlog.Debug("ACK start offset: %v", worker.processMsg.Event.Offset)
	err = worker.processMsg.Consumer.SetAckOffset(worker.processMsg.Event.Offset)
	if err != nil {
		mlog.Error("Failed to ack start offset: %v", err)
		return err
	}

	mlog.Debug("Snippet execution finished")
	worker.cleanup(output.Status)
	return nil
}

func (worker *SnippetRunWorker) Stop() {
	mlog.Debug("Stop worker signal received")

	if worker.interpreter != nil {
		worker.interpreter.Terminate()
	}
}

func (worker *SnippetRunWorker) cleanup(status string) {
	mlog.Debug("Cleaning up worker in status: %v", status)

	// This wait is needed to give msgq time to sync offset
	time.Sleep(time.Second * 5)

	worker.workers.Remove(worker.startEvent.InvocationId)

	if status == repl.SNIPPET_OUT_OF_MEMORY {
		mlog.Debug("Terminating process because of status: %v", status)
		os.Exit(1)
	}

	mlog.Debug("Worker cleanup complete")
}
