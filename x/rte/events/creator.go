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
	"time"
)

type EventsCreator struct{}

func NewEventsCreator() *EventsCreator {
	return &EventsCreator{}
}

func (e EventsCreator) CreateStartEvent(accountId string,
	event *SnippetStartEvent) (*RTEEvent, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	rteEvent, err := e.createEvent(SNIPPET_START_EVENT, accountId, data)
	if err != nil {
		return nil, err
	}

	return rteEvent, nil
}

func (e EventsCreator) CreateStopEvent(accountId string,
	event *SnippetStopEvent) (*RTEEvent, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	rteEvent, err := e.createEvent(SNIPPET_STOP_EVENT, accountId, data)
	if err != nil {
		return nil, err
	}

	return rteEvent, nil
}

func (e EventsCreator) CreateOutputEvent(accountId string,
	event *SnippetOutputEvent) (*RTEEvent, error) {

	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	rteEvent, err := e.createEvent(SNIPPET_OUTPUT_EVENT, accountId, data)
	if err != nil {
		return nil, err
	}

	return rteEvent, nil
}

func (e EventsCreator) createEvent(eventName string,
	accountId string,
	data []byte) (*RTEEvent, error) {
	rteEvent := RTEEvent{Event: eventName,
		AccountId: accountId,
		Timestamp: time.Now(),
		Data:      data}
	return &rteEvent, nil
}
