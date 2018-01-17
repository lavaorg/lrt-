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
	"encoding/json"
	"fmt"
	"github.com/orcaman/concurrent-map"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/service_master"
	"github.com/verizonlabs/northstar/pkg/kafka"
	"github.com/verizonlabs/northstar/pkg/rte/config"
	"github.com/verizonlabs/northstar/pkg/rte/repl"
	"github.com/verizonlabs/northstar/pkg/rte/rlimit"
	"github.com/verizonlabs/northstar/pkg/rte/stats"
	"github.com/verizonlabs/northstar/pkg/rte/topics"
	"github.com/verizonlabs/northstar/rte-lua/interpreter"
)

type EventHandler struct {
	workers        cmap.ConcurrentMap
	snippetManager SnippetManager
	serviceMaster  *service_master.ServiceMaster
	interpreter    repl.Interpreter
	ctrlTopic      string
}

func NewEventsHandler(rteType string) (*EventHandler, error) {
	snippetManager, err := NewSnippetManagerService(config.RTE_SERVICE_NAME,
		topics.RTE_OUTPUT_TOPIC)
	if err != nil {
		mlog.Error("NewSnippetManagerService failed: %v", err)
		return nil, err
	}

	interpreter, err := initInterpreter(rteType)
	if err != nil {
		mlog.Error("Failed to create snippet runner: %v", err)
		return nil, err
	}

	ctrlTopic, err := topics.GetCtrlTopicByType(rteType)
	if err != nil {
		mlog.Error("GetCtrlTopicByType failed: %v", err)
		return nil, err
	}

	return &EventHandler{workers: cmap.New(),
		snippetManager: snippetManager,
		serviceMaster:  service_master.New(1, config.WorkerQueueCapacity),
		interpreter:    interpreter,
		ctrlTopic:      ctrlTopic,
	}, nil
}

func (handler *EventHandler) Start() error {
	client, err := kafka.NewKafkaConsumer(config.RTE_SERVICE_NAME)
	if err != nil {
		mlog.Error("NewKafkaConsumer failed: %v", err)
		return err
	}

	err = client.ConsumeFromOnePartition(handler.ctrlTopic, handler.onReceiveMessage)
	if err != nil {
		mlog.Error("ConsumeFromOnePartition failed: %v", err)
		return err
	}

	return nil
}

func (handler *EventHandler) onReceiveMessage(msg *kafka.ProcessMsg) error {
	timer := stats.RTE.NewTimer("OnReceiveMessage")

	pErr := handler.processMessage(msg)
	if pErr != nil {
		timer.Stop()
		stats.ErrOnReceiveMessage.Incr()
		mlog.Error("processMessage failed: %v", pErr)

		cErr := msg.Consumer.SetAckOffset(msg.Event.Offset)
		if cErr != nil {
			mlog.Error("SetAckOffset failed: %v", cErr)
			return cErr
		}

		mlog.Debug("SetAckOffset complete: %v", msg.Event.Offset)
		return pErr
	}

	timer.Stop()
	stats.OnReceiveMessage.Incr()
	mlog.Debug("OnReceiveMessage finished")
	return nil
}

func (handler *EventHandler) processMessage(msg *kafka.ProcessMsg) error {
	var rteEvent RTEEvent

	err := json.Unmarshal(msg.Event.Value, &rteEvent)
	if err != nil {
		mlog.Error("RTEEvent unmarshal failed: %v", err)
		return err
	}

	mlog.Debug("Received processing event: %s", rteEvent.Event)
	switch rteEvent.Event {
	case SNIPPET_START_EVENT:
		var startEvent SnippetStartEvent
		err := json.Unmarshal(rteEvent.Data, &startEvent)
		if err != nil {
			mlog.Error("SnippetStartEvent unmarshal failed: %v", err)
			return err
		}

		worker := NewSnippetRunWorker(rteEvent.AccountId,
			handler.workers,
			handler.snippetManager,
			&startEvent,
			handler.interpreter,
			msg)
		handler.workers.Set(startEvent.InvocationId, worker)
		if err := handler.serviceMaster.Dispatch(config.RTE_SERVICE_NAME, worker); err != nil {
			mlog.Error("Dispatch failed: %v", err)
			return err
		}
	case SNIPPET_STOP_EVENT:
		mlog.Debug("Stop event received")
		var stopEvent SnippetStopEvent
		err := json.Unmarshal(rteEvent.Data, &stopEvent)
		if err != nil {
			mlog.Error("SnippetStopEvent unmarshal failed: %v", err)
			return err
		}

		err = handler.stopWorker(stopEvent.InvocationId)
		if err != nil {
			return err
		}

		mlog.Debug("ACK stop offset: %v", msg.Event.Offset)
		err = msg.Consumer.SetAckOffset(msg.Event.Offset)
		if err != nil {
			mlog.Error("SetAckOffset failed on stop offset: %v", err)
			return err
		}
	default:
		err = fmt.Errorf("Unknown event: %s", rteEvent.Event)
		mlog.Error(err.Error())
		return err
	}

	return nil
}

func (handler *EventHandler) stopWorker(invocationId string) error {
	worker, ok := handler.workers.Get(invocationId)
	if !ok {
		return fmt.Errorf("Worker not found for invocation: %v", invocationId)
	}

	snippetWorker := worker.(*SnippetRunWorker)
	if snippetWorker == nil {
		return fmt.Errorf("SnippetRunWorker is null")
	}

	snippetWorker.Stop()
	return nil
}

func initInterpreter(runtime string) (repl.Interpreter, error) {
	switch runtime {
	case repl.Lua:
		return interpreter.NewLuaInterpreter(rlimit.NewLuaResourceLimit()), nil
	default:
		return nil, fmt.Errorf("Unknown runtime received: %v", runtime)
	}
}
