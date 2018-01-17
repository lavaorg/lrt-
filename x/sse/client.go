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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	CONNECTING = iota
	OPEN       = iota
	CLOSED     = iota
)

const (
	SSE_STREAM_STATE_CONNECTED    = "SSE_STREAM_STATE_CONNECTED"
	SSE_STREAM_STATE_DISCONNECTED = "SSE_STREAM_STATE_DISCONNECTED"
)

/* This is a callback function that gets invoked before
   reconnecting to the event source. This allow the user
   to change or update the event source url if there are
   more than one instances of the event source
*/
type EventSourceUpdateFunc func() (string, error)

type EventSource struct {
	evtSrcUpdateFunc   EventSourceUpdateFunc
	eventChannel       chan *Event
	errorChannel       chan error
	method             string
	request            []byte
	readyState         uint16
	responseReader     *bufio.Reader
	response           *http.Response
	closeFunc          connectionCloseFunc
	reconnectionTime   time.Duration
	lastConnectionTime time.Time
	lastEventId        *string
	authClient         AuthenticationClient
	lock               *sync.Mutex
}

type AuthenticationClient interface {
	Do(request *http.Request) (client *http.Client, resp *http.Response, err error)
}

type BasicAuthClient struct {
	UserName string
	Password string
}

func (bac *BasicAuthClient) Do(request *http.Request) (client *http.Client, resp *http.Response, err error) {
	client = &http.Client{}
	request.SetBasicAuth(bac.UserName, bac.Password)
	resp, err = client.Do(request)
	return client, resp, err
}

type connectionCloseFunc func()

func NewEventSource(evtSrcUpdateFunc EventSourceUpdateFunc) (es *EventSource, err error) {
	return NewEventSourceWithAllOptions(evtSrcUpdateFunc, "GET", nil, nil, 100*time.Millisecond, nil)
}

func NewEventSourceWithBasicAuth(evtSrcUpdateFunc EventSourceUpdateFunc, username string, password string) (es *EventSource, err error) {
	bac := &BasicAuthClient{
		UserName: username,
		Password: password,
	}
	return NewEventSourceWithAllOptions(evtSrcUpdateFunc, "GET", nil, nil, 100*time.Millisecond, bac)
}

func NewEventSourceWithEventId(evtSrcUpdateFunc EventSourceUpdateFunc, lastEventId *string) (es *EventSource, err error) {
	return NewEventSourceWithAllOptions(evtSrcUpdateFunc, "GET", nil, lastEventId, 100*time.Millisecond, nil)
}

func NewEventSourceWithRequest(evtSrcUpdateFunc EventSourceUpdateFunc, method string, request []byte, lastEventId *string) (es *EventSource, err error) {
	return NewEventSourceWithAllOptions(evtSrcUpdateFunc, method, request, lastEventId, 100*time.Millisecond, nil)
}

func NewEventSourceWithRequestAndBasicAuth(evtSrcUpdateFunc EventSourceUpdateFunc,
	method string,
	request []byte,
	username string,
	password string,
	lastEventId *string) (es *EventSource, err error) {

	bac := &BasicAuthClient{
		UserName: username,
		Password: password,
	}
	return NewEventSourceWithAllOptions(evtSrcUpdateFunc, method, request, lastEventId, 100*time.Millisecond, bac)
}

func NewEventSourceWithAllOptions(evtSrcUpdateFunc EventSourceUpdateFunc,
	method string,
	request []byte,
	lastEventId *string,
	reconnectionTime time.Duration,
	authClient AuthenticationClient) (es *EventSource, err error) {

	if evtSrcUpdateFunc == nil {
		mlog.Error("Event source update function must be specified")
		return nil, errors.New("Event source update function must be specified")
	}

	err = validateMethod(method)

	if err != nil {
		return nil, err
	}

	eventSource := &EventSource{evtSrcUpdateFunc: evtSrcUpdateFunc,
		eventChannel:       make(chan *Event, 1),
		errorChannel:       make(chan error, 1),
		method:             method,
		request:            request,
		readyState:         CONNECTING,
		reconnectionTime:   reconnectionTime,
		lastConnectionTime: time.Now(),
		authClient:         authClient,
		lock:               &sync.Mutex{},
	}

	return eventSource, nil
}

func validateMethod(method string) error {
	ucMethod := strings.ToUpper(method)
	if ucMethod == "GET" ||
		ucMethod == "POST" ||
		ucMethod == "PUT" {
		return nil
	}
	return errors.New(fmt.Sprintf("Unsupported HTTP method: %s", method))
}

func (evSrc *EventSource) Start() {

	evSrc.lock.Lock()
	defer evSrc.lock.Unlock()
	if evSrc.readyState == CLOSED {
		return
	}
	go func() {

		for {

			evSrc.checkAndRestoreConnection()
			err := evSrc.readMessage()

			if evSrc.readyState == CLOSED {
				return
			}

			if err != nil {
				mlog.Error("Error reading message %s", err.Error())
				evSrc.responseReader = nil
				evSrc.response.Body.Close()
				evSrc.readyState = CLOSED
				disconnectedStr := SSE_STREAM_STATE_DISCONNECTED
				event := NewEvent(nil, &disconnectedStr, disconnectedStr, nil)
				evSrc.eventChannel <- event
			}
		}
	}()
}

func (evSrc *EventSource) Stop() {
	evSrc.lock.Lock()
	defer evSrc.lock.Unlock()
	evSrc.readyState = CLOSED
	if evSrc.closeFunc != nil {
		evSrc.closeFunc()
	}
	close(evSrc.errorChannel)
	close(evSrc.eventChannel)
}

func (evSrc *EventSource) GetLastEventId() *string {
	return evSrc.lastEventId
}

func (evSrc *EventSource) GetEventChannel() chan *Event {
	return evSrc.eventChannel
}

func (evSrc *EventSource) GetErrorChannel() chan error {
	return evSrc.errorChannel
}

func (evSrc *EventSource) initializeConnecion() (reader *bufio.Reader, err error) {
	evSrc.lock.Lock()
	evSrc.readyState = CONNECTING
	evSrc.lock.Unlock()
	if time.Since(evSrc.lastConnectionTime) < evSrc.reconnectionTime {
		time.Sleep(evSrc.reconnectionTime - time.Since(evSrc.lastConnectionTime))
	}
	evSrc.lastConnectionTime = time.Now()

	sourceUrl, err := evSrc.evtSrcUpdateFunc()
	if err != nil {
		mlog.Error("Failed to get event source URL %v", err)
		return nil, err
	}

	_, err = url.Parse(sourceUrl)
	if err != nil {
		mlog.Error("Invalid error source %v", sourceUrl)
		return nil, err
	}

	mlog.Debug("Connecting to %s %s\n", evSrc.method, sourceUrl)

	var request *http.Request

	if evSrc.request != nil {
		request, err = http.NewRequest(evSrc.method, sourceUrl, bytes.NewBuffer(evSrc.request))
	} else {
		request, err = http.NewRequest(evSrc.method, sourceUrl, nil)
	}

	if err != nil {
		return nil, err
	}

	header := request.Header

	header.Set("Accept", "text/event-stream")
	if evSrc.lastEventId != nil {
		header.Set("Last-Event-ID", *evSrc.lastEventId)
	}

	var client *http.Client
	var resp *http.Response

	if evSrc.authClient == nil {
		client = &http.Client{}
		resp, err = client.Do(request)
	} else {
		client, resp, err = evSrc.authClient.Do(request)
	}

	if err != nil {
		return nil, err
	}

	responseReader := bufio.NewReader(resp.Body.(io.Reader))
	evSrc.lock.Lock()
	evSrc.readyState = OPEN
	evSrc.response = resp
	evSrc.closeFunc = func() {
		resp.Body.Close()
	}
	evSrc.lock.Unlock()
	connectedStr := SSE_STREAM_STATE_CONNECTED
	event := NewEvent(nil, &connectedStr, connectedStr, nil)
	evSrc.eventChannel <- event
	return responseReader, nil
}

func (evSrc *EventSource) checkAndRestoreConnection() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New("This EventSource is closed.")
			mlog.Error("Error writing to error stream %s", e)
		}
	}()
	for evSrc.responseReader == nil {
		reader, e := evSrc.initializeConnecion()
		if e != nil {
			evSrc.errorChannel <- e
		} else {
			evSrc.responseReader = reader
		}
	}
	return nil
}

func (evSrc *EventSource) readMessage() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New("This EventSource is closed.")
			mlog.Error("Error writing to error stream %s", e)
		}
	}()
	if evSrc.responseReader == nil {
		return err
	}

	var id *string
	var data *bytes.Buffer
	var event string
	var retry *uint32

	event = "message"

	for {
		line, err := readLine(evSrc.responseReader)
		if err != nil {

			//This is a fatal error. In this case we report the error and stop.
			evSrc.errorChannel <- err
			return err
		}
		trimLine := strings.TrimLeft(*line, "\t ")

		commentIndex := strings.Index(*line, ":")
		if commentIndex == 0 {
			continue
		}
		if len(trimLine) == 0 {
			//End of event
			if data != nil && data.Len() > 0 {
				if id != nil {
					evSrc.lastEventId = id
				}

				if retry != nil {
					evSrc.reconnectionTime = time.Duration(*retry) * time.Millisecond
				}

				event := NewEvent(id, &event, data.String(), retry)
				evSrc.eventChannel <- event
			}

			return err
		}
		if strings.HasPrefix(trimLine, "id:") {
			id = getFieldValue(&trimLine, "id:")
		} else if strings.HasPrefix(trimLine, "data:") {
			dataStr := getFieldValue(&trimLine, "data:")
			if dataStr != nil {
				if data == nil {
					data = bytes.NewBufferString(*dataStr)
				} else {
					data.WriteString("\n")
					data.WriteString(*dataStr)
				}
			}
		} else if strings.HasPrefix(trimLine, "event:") {
			eventStr := getFieldValue(&trimLine, "event:")
			if eventStr != nil {
				event = *eventStr
			}
		} else if strings.HasPrefix(trimLine, "retry:") {
			retryStr := getFieldValue(&trimLine, "retry:")
			retryValue64, err := strconv.ParseUint(*retryStr, 10, 32)
			if err == nil {
				retryValue32 := uint32(retryValue64)
				retry = &retryValue32
			}

		}

	}
	return nil

}

func getFieldValue(line *string, field string) *string {
	fieldValue := strings.TrimLeft(strings.TrimPrefix(*line, field), "\t ")
	if len(fieldValue) > 0 && len(fieldValue) < len(*line) {
		return &fieldValue
	}
	return nil
}

func readLine(reader *bufio.Reader) (fullLine *string, err error) {
	line, prefix, err := reader.ReadLine()

	if err != nil {
		return nil, err
	}

	if !prefix {
		strLine := string(line)
		return &strLine, nil
	}

	var buffer bytes.Buffer
	buffer.Write(line)

	for prefix {
		line, prefix, err = reader.ReadLine()

		if err != nil {
			return nil, err
		}

		buffer.Write(line)
	}
	strLine := buffer.String()
	return &strLine, nil
}
