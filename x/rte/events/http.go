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
	"fmt"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/northstarapi/internalClient"
	"github.com/verizonlabs/northstar/northstarapi/model"
	"github.com/verizonlabs/northstar/pkg/rte/config"
	"github.com/verizonlabs/northstar/pkg/rte/stats"
)

type HttpEventsProducer struct {
	eventsCreator *EventsCreator
}

func NewHttpEventsProducer() (*HttpEventsProducer, error) {
	return &HttpEventsProducer{eventsCreator: NewEventsCreator()}, nil
}

func (e HttpEventsProducer) SnippetOutput(accountId string, output *SnippetOutputEvent) error {
	nsAPIHostPort, err := config.GetNorthStarApiHostPort()
	if err != nil {
		return err
	}

	timer := stats.RTE.NewTimer("SnippetOutputCallbackTimer")
	response := model.ExecutionResponse{AccountID: accountId,
		InvocationID:     output.InvocationId,
		RteID:            output.RTEId,
		SnippetID:        output.SnippetId,
		Status:           output.Status,
		ErrorDescription: output.ErrorDescription,
		StartedOn:        output.StartedOn,
		ElapsedTime:      output.ElapsedTime,
		FinishedOn:       output.FinishedOn,
		Callback:         output.Callback}

	mlog.Debug("Creating client for NS API endpoint: %v", nsAPIHostPort)
	client, err := internalClient.NewInternalClient("http", nsAPIHostPort)
	if err != nil {
		timer.Stop()
		stats.ErrSnippetOutputCallback.Incr()
		return err
	}

	mlog.Debug("Calling execution callback")
	mErr := client.ExecutionCallback(&response)
	if mErr != nil {
		timer.Stop()
		stats.ErrSnippetOutputCallback.Incr()
		return fmt.Errorf(mErr.Error())
	}

	mlog.Debug("Execution callback finished")
	timer.Stop()
	stats.SnippetOutputCallback.Incr()
	return nil
}
