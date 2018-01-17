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

	"errors"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
	"github.com/verizonlabs/northstar/data/invocations/client"
	"github.com/verizonlabs/northstar/data/invocations/model"
	"github.com/verizonlabs/northstar/pkg/rte/config"
	"github.com/verizonlabs/northstar/pkg/rte/repl"
	"github.com/verizonlabs/northstar/pkg/rte/stats"
)

type SnippetManager interface {
	SnippetStart(accountId string, start *SnippetStartEvent) (string, error)
	SnippetStop(accountId string, partition int, stop *SnippetStopEvent) error
	SnippetOutput(accountId string, start *SnippetStartEvent, output *repl.Output) error
	UpdateInvocation(accountId string, invocationId string, partition int32, status string) error
}

type SnippetManagerService struct {
	topicName          string
	rteId              string
	msgQ               msgq.MessageQueue
	kafkaProducer      msgq.MsgQProducer
	httpEventsProducer *HttpEventsProducer
	eventsCreator      *EventsCreator
	invocationClient   client.Client
}

func NewSnippetManagerService(serviceName string, topicName string) (SnippetManager, error) {
	rteId, err := config.GetRTEId()
	if err != nil {
		return nil, err
	}

	msgQ, err := msgq.NewMsgQ(serviceName, nil, nil)
	if err != nil {
		return nil, err
	}

	kafkaProducer, err := msgQ.NewProducer(&msgq.ProducerConfig{
		TopicName:   topicName,
		Partitioner: msgq.RoundRobinPartitioner,
	})
	if err != nil {
		return nil, err
	}

	invocationClient, err := client.NewInvocationClient()
	if err != nil {
		return nil, err
	}

	httpEventsProducer, err := NewHttpEventsProducer()
	if err != nil {
		return nil, err
	}

	return &SnippetManagerService{topicName: topicName,
		rteId:              rteId,
		msgQ:               msgQ,
		kafkaProducer:      kafkaProducer,
		httpEventsProducer: httpEventsProducer,
		eventsCreator:      NewEventsCreator(),
		invocationClient:   invocationClient}, nil
}

func (n *SnippetManagerService) SnippetStart(accountId string,
	start *SnippetStartEvent) (string, error) {
	timer := stats.RTE.NewTimer("SnippetStartTimer")
	invocation := &model.InvocationData{SnippetId: start.SnippetId,
		MainFn:   start.MainFn,
		Runtime:  start.Runtime,
		Timeout:  start.Timeout,
		Memory:   start.Memory,
		Args:     start.Args,
		URL:      start.URL,
		Code:     start.Code,
		Callback: start.Callback,
		Status:   SNIPPET_START_EVENT}

	invocationId, mErr := n.invocationClient.AddInvocation(accountId, invocation)
	if mErr != nil {
		timer.Stop()
		stats.ErrSnippetStart.Incr()
		return "", errors.New(mErr.Error())
	}

	start.InvocationId = invocationId
	data, err := n.eventsCreator.CreateStartEvent(accountId, start)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStart.Incr()
		return "", err
	}

	output, err := json.Marshal(data)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStart.Incr()
		return "", err
	}

	mlog.Debug("Account %s sending to Kafka topic %s: %s", accountId, n.topicName, string(output))
	err = n.kafkaProducer.Send(output)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStart.Incr()
		return "", err
	}

	mlog.Debug("Message sent to Kafka")
	timer.Stop()
	stats.SnippetStart.Incr()
	return invocationId, nil
}

func (service *SnippetManagerService) SnippetStop(accountId string,
	partition int,
	stop *SnippetStopEvent) error {
	timer := stats.RTE.NewTimer("SnippetStopTimer")

	data, err := service.eventsCreator.CreateStopEvent(accountId, stop)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStop.Incr()
		return err
	}

	output, err := json.Marshal(data)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStop.Incr()
		return err
	}

	producer, err := service.msgQ.NewProducer(&msgq.ProducerConfig{
		TopicName:   service.topicName,
		Partitioner: msgq.ManualPartitioner,
	})
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStop.Incr()
		return err
	}

	mlog.Debug("Account %s sending to Kafka topic %v, partition %v, output: %v",
		accountId, service.topicName, partition, string(output))
	err = producer.SendToPartition(int32(partition), output)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetStop.Incr()
		return err
	}

	timer.Stop()
	stats.SnippetStop.Incr()
	return nil
}

func (service *SnippetManagerService) SnippetOutput(accountId string,
	startEvent *SnippetStartEvent,
	output *repl.Output) error {
	timer := stats.RTE.NewTimer("SnippetOutputTimer")

	// Store output in data service
	err := service.storeInvocationOutput(accountId,
		startEvent.InvocationId,
		startEvent.SnippetId,
		output)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetOutput.Incr()
		mlog.Error("Failed to store invocation output: %v", err)
		return err
	}

	// Send event to Kafka
	outputEvent := &SnippetOutputEvent{
		InvocationId:     startEvent.InvocationId,
		SnippetId:        startEvent.SnippetId,
		RTEId:            service.rteId,
		StartedOn:        output.StartedOn,
		FinishedOn:       output.FinishedOn,
		ElapsedTime:      output.ElapsedTime,
		Status:           output.Status,
		ErrorDescription: output.ErrorDescr,
		Callback:         startEvent.Callback}
	data, err := service.eventsCreator.CreateOutputEvent(accountId, outputEvent)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetOutput.Incr()
		return err
	}

	outputByte, err := json.Marshal(data)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetOutput.Incr()
		return err
	}

	mlog.Debug("Sending to Kafka topic %s: %s", service.topicName, string(outputByte))
	err = service.kafkaProducer.Send(outputByte)
	if err != nil {
		stats.ErrSnippetOutput.Incr()
		return err
	}

	mlog.Debug("Output sent to Kafka")
	if startEvent.Callback != "" {
		mlog.Debug("Detected callback: %v", startEvent.Callback)
		err := service.httpEventsProducer.SnippetOutput(accountId, outputEvent)
		if err != nil {
			timer.Stop()
			stats.ErrSnippetOutput.Incr()
			mlog.Error("Failed to trigger HTTP callback: %v", err)
			return err
		}
	}

	timer.Stop()
	stats.SnippetOutput.Incr()
	return nil
}

func (event *SnippetManagerService) storeInvocationOutput(accountId string,
	invocationId string,
	snippetId string,
	output *repl.Output) error {
	timer := stats.RTE.NewTimer("StoreInvocationOutputTimer")

	invocation := &model.InvocationData{
		Id:          invocationId,
		SnippetId:   snippetId,
		RTEId:       event.rteId,
		StartedOn:   output.StartedOn,
		FinishedOn:  output.FinishedOn,
		ElapsedTime: output.ElapsedTime.Seconds(),
		Stdout:      output.Stdout,
		Result:      output.Result,
		Status:      output.Status,
		ErrorDescr:  output.ErrorDescr}
	mlog.Debug("Storing invocation output for account id %s, "+
		"invocation id %s, "+
		"startedon: %s, "+
		"finishedon: %s, "+
		"elapsedtime: %f, "+
		"stdout: %s, "+
		"result: %s, "+
		"status: %s, "+
		"error descr: %s",
		accountId,
		invocation.Id,
		invocation.StartedOn,
		invocation.FinishedOn,
		invocation.ElapsedTime,
		invocation.Stdout,
		invocation.Result,
		invocation.Status,
		invocation.ErrorDescr)
	mErr := event.invocationClient.UpdateInvocation(accountId, invocationId, invocation)
	if mErr != nil {
		mlog.Error("Failed to store invocation output: %v", mErr)
		timer.Stop()
		stats.ErrStoreInvocationOutput.Incr()
		return errors.New(mErr.Error())
	}

	mlog.Debug("Invocation updated")
	timer.Stop()
	stats.StoreInvocationOutput.Incr()
	return nil
}

func (event *SnippetManagerService) UpdateInvocation(accountId string,
	invocationId string,
	partition int32,
	status string) error {
	timer := stats.RTE.NewTimer("UpdateInvocationStatus")

	mlog.Debug("Updating invocation: %v", invocationId)
	invocation := &model.InvocationData{Partition: int(partition), Status: status}
	mErr := event.invocationClient.UpdateInvocation(accountId, invocationId, invocation)
	if mErr != nil {
		mlog.Error("Failed to store invocation output: %v", mErr)
		timer.Stop()
		stats.ErrUpdateInvocationStatus.Incr()
		return errors.New(mErr.Error())
	}

	timer.Stop()
	stats.UpdateInvocationStatus.Incr()
	mlog.Debug("Updated invocation %s with partition number %v and status: %v",
		invocationId, partition, status)
	return nil
}
