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

package sse

import (
	"bytes"
	"net/http"
	"regexp"
	"testing"
)

type HttpResponseWriterMock struct {
	buffer bytes.Buffer
	header http.Header
}

func (hrwm *HttpResponseWriterMock) Header() http.Header {
	return hrwm.header
}

func (hrwm *HttpResponseWriterMock) Write(bytes []byte) (int, error) {
	return hrwm.buffer.Write(bytes)
}

func (hrwm *HttpResponseWriterMock) WriteHeader(int) {

}

func (hrwm *HttpResponseWriterMock) Flush() {

}

func TestAddEventStreamTesting(t *testing.T) {
	server := &Server{
		addEventStreamChannel: make(chan *EventStreamConnection, 2),
	}

	evs := &EventStreamConnection{}
	err := server.AddEventStreamConnection(evs)

	if err != nil {
		t.Error("Error Adding stream ", err.Error())
		return
	}

	success := false

	select {
	case _ = <-server.addEventStreamChannel:
		success = true
	default:
		success = false
	}

	if !success {
		t.Error("EventStreamConnection was not added.")
	}
}

func TestAddEventStreamTestingErrorCase(t *testing.T) {
	server := &Server{
		addEventStreamChannel: make(chan *EventStreamConnection, 1),
	}

	evs := &EventStreamConnection{}
	err := server.AddEventStreamConnection(evs)

	if err != nil {
		t.Error("Error Adding stream ", err.Error())
	}

	err = server.AddEventStreamConnection(evs)

	if err == nil {
		t.Error("There should be error adding event stream")
	}
}

func TestRemoveEventStreamTesting(t *testing.T) {
	server := &Server{
		removeEventStreamChannel: make(chan *EventStreamConnection, 2),
	}

	evs := &EventStreamConnection{}
	err := server.RemoveEventStreamConnection(evs)

	if err != nil {
		t.Error("Error Removing stream ", err.Error())
		return
	}

	success := false

	select {
	case _ = <-server.removeEventStreamChannel:
		success = true
	default:
		success = false
	}

	if !success {
		t.Error("EventStreamConnection was not removed.")
	}
}

func TestRemoveEventStreamTestingErrorCase(t *testing.T) {
	server := &Server{
		removeEventStreamChannel: make(chan *EventStreamConnection, 1),
	}

	evs := &EventStreamConnection{}
	err := server.RemoveEventStreamConnection(evs)

	if err != nil {
		t.Error("Error Removing stream ", err.Error())
	}

	err = server.RemoveEventStreamConnection(evs)

	if err == nil {
		t.Error("There should be error removing event stream")
	}
}

func TestSendEvent(t *testing.T) {
	responseWriter := HttpResponseWriterMock{
		header: make(map[string][]string),
		buffer: bytes.Buffer{},
	}
	evs := &EventStreamConnection{
		responseWriter: &responseWriter,
	}

	eventId := "test_id"
	eventType := "event_type"
	eventData := "data"
	eventRetry := uint32(10)

	event := &Event{
		id:    &eventId,
		event: &eventType,
		data:  eventData,
		retry: &eventRetry,
	}

	err := evs.sendEventInternal(event)
	if err != nil {
		t.Error("Error sending event ", err.Error())
		return
	}

	serializedData := string(responseWriter.buffer.Bytes())

	lines := regexp.MustCompile("\r\n|\n\r|\n|\r").Split(serializedData, -1)

	if lines[0] != "id: test_id" {
		t.Error("Invalid serialization.")
	}

	if lines[1] != "event: event_type" {
		t.Error("Invalid serialization.")
	}

	if lines[2] != "data: data" {
		t.Error("Invalid serialization.")
	}

	if lines[3] != "retry: 10" {
		t.Error("Invalid serialization.")
	}

	if lines[4] != "" {
		t.Error("Invalid serialization.")
	}
}
