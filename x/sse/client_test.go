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
	"bufio"
	"bytes"
	"testing"
)

func TestValidateMethodGET(t *testing.T) {
	err := validateMethod("GET")
	if err != nil {
		t.Error("GET should be acceptable")
	}
}

func TestValidateMethodPUT(t *testing.T) {
	err := validateMethod("PUT")
	if err != nil {
		t.Error("PUT should be acceptable")
	}
}

func TestValidateMethodPOST(t *testing.T) {
	err := validateMethod("POST")
	if err != nil {
		t.Error("POST should be acceptable")
	}
}

func TestReadLine(t *testing.T) {
	origLine := "This is a line"
	buf := bytes.NewBuffer([]byte(origLine + "\n"))
	bufReader := bufio.NewReader(buf)
	line, err := readLine(bufReader)

	if err != nil {
		t.Error("Error occured while processing line %s", err.Error())
		return
	}

	if line == nil {
		t.Error("Line is nil")
		return
	}

	if *line != origLine {
		t.Errorf("The lines \"%s\" and \"%s\" doesn't match.", *line, origLine)
	}

}

func TestReadLineTwoLines(t *testing.T) {
	firstLine := "This is a line"
	secondLine := "This is second line"
	buf := bytes.NewBuffer([]byte(firstLine + "\n" + secondLine))
	bufReader := bufio.NewReader(buf)
	line, err := readLine(bufReader)

	if err != nil {
		t.Error("Error occured while processing line %s", err.Error())
		return
	}

	if line == nil {
		t.Error("Line is nil")
		return
	}

	if *line != firstLine {
		t.Errorf("The lines \"%s\" and \"%s\" doesn't match.", *line, firstLine)
	}

}

func TestReadError(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	bufReader := bufio.NewReader(buf)
	line, err := readLine(bufReader)

	if err == nil {
		t.Error("No error occured while processing errored reader")
		return
	}

	if line != nil {
		t.Error("Line produced while processing errored reader")
	}

}

func TestGetFieldValue(t *testing.T) {
	fieldName := "id:"
	value := "value"
	line := fieldName + " " + value
	fieldValue := getFieldValue(&line, fieldName)
	if fieldValue == nil {
		t.Error("Field value shouldn't be nil.")
		return
	}

	if *fieldValue != value {
		t.Errorf("Invalid field value %s, Original Value %s", *fieldValue, value)
	}

}

func TestGetFieldValueInvalidValue(t *testing.T) {
	fieldName := "id:"
	value := "value"
	line := "data:" + " " + value
	fieldValue := getFieldValue(&line, fieldName)
	if fieldValue != nil {
		t.Error("Field value should be nil.")
	}

}

func TestReadMessage(t *testing.T) {
	message := "id: messageId\ndata: this is data\nretry: 13\nevent: message\n\r\n"
	buf := bytes.NewBuffer([]byte(message))
	responseReader := bufio.NewReader(buf)

	evSrc := &EventSource{
		responseReader: responseReader,
		eventChannel:   make(chan *Event, 2),
		errorChannel:   make(chan error, 2),
	}

	evSrc.readMessage()
	messageReceived := false

	select {
	case event := <-evSrc.eventChannel:
		if event.GetId() == nil {
			t.Error("Event id is  nil")
			return
		}

		if *event.GetId() != "messageId" {
			t.Error("Event id should be messageId, but is %s", *event.GetId())
			return
		}

		if event.GetData() != "this is data" {
			t.Error("Data should be \"this is data\" but is %s", event.GetData())
			return
		}

		if event.GetRetry() == nil {
			t.Error("Retry is nil")
			return
		}

		if *event.GetRetry() != 13 {
			t.Error("Retry should be 13 but is %d", *event.GetRetry())
		}

		if event.GetEvent() == nil {
			t.Error("Event is nil")
			return
		}

		if *event.GetEvent() != "message" {
			t.Error("Event should be \"message\" but is %s", *event.GetEvent())
			return
		}
		messageReceived = true
	default:
		messageReceived = false
	}

	if !messageReceived {
		t.Error("Message not processed")
	}
}

func TestReadMessageWithJustData(t *testing.T) {
	message := "data: this is data\n\r\n"
	buf := bytes.NewBuffer([]byte(message))
	responseReader := bufio.NewReader(buf)

	evSrc := &EventSource{
		responseReader: responseReader,
		eventChannel:   make(chan *Event, 2),
		errorChannel:   make(chan error, 2),
	}

	evSrc.readMessage()
	messageReceived := false

	select {
	case event := <-evSrc.eventChannel:
		if event.GetId() != nil {
			t.Error("Event id should be nil")
			return
		}

		if event.GetData() != "this is data" {
			t.Error("Data should be \"this is data\" but is %s", event.GetData())
			return
		}

		if event.GetRetry() != nil {
			t.Error("Retry should be nil")
			return
		}

		if event.GetEvent() == nil {
			t.Error("Event is nil")
			return
		}

		if *event.GetEvent() != "message" {
			t.Error("Event should be \"message\" but is %s", *event.GetEvent())
			return
		}
		messageReceived = true
	default:
		messageReceived = false
	}

	if !messageReceived {
		t.Error("Message not processed")
	}
}
