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
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"strings"
)

// EventStreamConnection abstracts the server side SSE connection.
// It exposes methods to send Event messages to the connected client.
type EventStreamConnection struct {
	msgs           chan interface{}
	closeNotify    chan bool
	responseWriter http.ResponseWriter
	remoteAddress  string
	lastEventId    string
	id             uint64
}

//Server is SSE server. This handle all connections from the SSE clients
type Server struct {
	Addr                     string                   // Address onwhich  this server listens on
	eventStreams             []*EventStreamConnection // List of currently held connections
	addEventStreamChannel    chan *EventStreamConnection
	removeEventStreamChannel chan *EventStreamConnection
	BufferSize               uint32 // Buffer size for messages
	MaxConnections           uint32 // Max number of concurrent connections this server can hold at a time.
	ConnectionQueueSize      uint32 // Max number of new unprocessed connections this server can have
	idCount                  uint64 // Connection ID Counter
}

//Event represents and SSE from http://www.w3.org/TR/2015/REC-eventsource-20150203/

type Event struct {
	event *string
	data  string
	id    *string
	retry *uint32
}

type HandlerFunc func(eventStreamConnection *EventStreamConnection, req *http.Request) error

func NewServer(config *Config) *Server {

	server := &Server{
		Addr: config.Addr,
		addEventStreamChannel:    make(chan *EventStreamConnection, config.ConnectionQueueSize),
		removeEventStreamChannel: make(chan *EventStreamConnection, config.MaxConcurrentConectionsAllowed),
		BufferSize:               config.BufferSize,
		MaxConnections:           config.MaxConcurrentConectionsAllowed,
		ConnectionQueueSize:      config.ConnectionQueueSize,
		idCount:                  0,
	}
	return server
}

func (server *Server) GetNextId() uint64 {
	id := server.idCount
	server.idCount++
	return id
}

func (server *Server) AddEventStreamConnection(eventStreamConnection *EventStreamConnection) error {
	if eventStreamConnection == nil {
		return errors.New("EventStreamConnection is nil")
	}
	select {
	case server.addEventStreamChannel <- eventStreamConnection:
	default:
		return errors.New("Max Connection Queue Size Exceeded.")
	}
	return nil
}

func (server *Server) RemoveEventStreamConnection(eventStreamConnection *EventStreamConnection) error {
	if eventStreamConnection == nil {
		return errors.New("EventStreamConnection is nil")
	}
	select {
	case server.removeEventStreamChannel <- eventStreamConnection:
	default:
		return errors.New("Max Concurrent Connection Exceeded.")
	}
	return nil
}

func (server *Server) Start() error {
	mlog.Debug("Listening on %s", server.Addr)
	return http.ListenAndServe(server.Addr, nil)
}

func (server *Server) AddRoute(route string, method string, handlerFunc HandlerFunc) error {
	if len(route) == 0 {
		return errors.New("Route is empty")
	}

	if len(method) == 0 {
		return errors.New("Method is empty")
	}

	ucMethod := strings.ToUpper(method)

	if ucMethod != "GET" && ucMethod != "POST" {
		return errors.New("Unsupported Method")
	}

	if handlerFunc == nil {
		return errors.New("HandlerFunc is mandatory")
	}

	http.HandleFunc(route, func(w http.ResponseWriter, req *http.Request) {
		noOfConnections := len(server.eventStreams)
		if uint32(noOfConnections) > server.MaxConnections {
			mlog.Error("Closing connection from %s because max connections exceeded.", req.RemoteAddr)
			return
		}

		if req.Method != method {
			mlog.Error("Invalid method, Closing the connection.", req.RemoteAddr)
			http.NotFound(w, req)
			return
		}

		eventStreamConnection := &EventStreamConnection{
			msgs:           make(chan interface{}, server.BufferSize),
			closeNotify:    make(chan bool, 1),
			responseWriter: w,
			remoteAddress:  req.RemoteAddr,
			id:             server.GetNextId(),
			lastEventId:    req.Header.Get("Last-Event-ID"),
		}

		server.AddEventStreamConnection(eventStreamConnection)

		err := handlerFunc(eventStreamConnection, req)

		if err != nil {
			mlog.Error("Error %s occured. Closing the connection %s", err.Error(), req.RemoteAddr)
			eventStreamConnection.Close()

		}

		h := w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		h.Set("Connection", "keep-alive")
		h.Set("Access-Control-Allow-Origin", "*")

		// response is not flushed here..
		// This gives us an option to close the response with an error code later. But had to be done before the first message is sent.

		eventStreamConnection.Run()
		server.RemoveEventStreamConnection(eventStreamConnection)

	})
	return nil
}

func NewEvent(id *string, event *string, data string, retry *uint32) *Event {
	return &Event{
		id:    id,
		event: event,
		data:  data,
		retry: retry,
	}
}

func (evt *Event) Validate() error {
	if len(evt.data) == 0 {
		return errors.New("Invalid Event: Data field is mandatory")
	}

	if evt.id != nil && len(*evt.id) > 0 && (strings.Contains(*evt.id, "\n") || strings.Contains(*evt.id, "\r")) {
		return errors.New("Invalid Event: Event Id field cannot have CR or LF")
	}

	if evt.event != nil && len(*evt.event) > 0 && (strings.Contains(*evt.event, "\n") || strings.Contains(*evt.event, "\r")) {
		return errors.New("Invalid Event: Event field cannot have CR or LF")
	}
	return nil

}

func (evt *Event) GetId() *string {
	return evt.id
}

func (evt *Event) GetEvent() *string {
	return evt.event
}

func (evt *Event) GetData() string {
	return evt.data
}

func (evt *Event) GetRetry() *uint32 {
	return evt.retry
}

func (eventStreamConnection *EventStreamConnection) Run() {
	closeNotifier := eventStreamConnection.responseWriter.(http.CloseNotifier)

	for {
		select {
		case msg := <-eventStreamConnection.msgs:
			switch msg := msg.(type) {
			case bool:
				mlog.Debug("Closing Event Stream Connection %d\n", eventStreamConnection.id)
				eventStreamConnection.cleanUp()
				return

			case *Event:
				err := eventStreamConnection.sendEventInternal(msg)
				if err != nil {
					mlog.Error("Closing EventStream %d for error %s\n", eventStreamConnection.id, err.Error())
					eventStreamConnection.cleanUp()
					return
				}
			}

		case _ = <-closeNotifier.CloseNotify():
			mlog.Info("Closing EventStream %d because remote end has shutdown.\n", eventStreamConnection.id)
			eventStreamConnection.closeNotify <- true
			eventStreamConnection.cleanUp()

			return
		}
	}
}

func (eventStreamConnection *EventStreamConnection) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {

			err = errors.New("This stream is already closed.")
		}
	}()
	select {
	case eventStreamConnection.msgs <- true:
	default:
		return errors.New("Can't close the stream. Buffer is full.")
	}
	return nil
}

func (eventStreamConnection *EventStreamConnection) GetId() uint64 {
	return eventStreamConnection.id
}

func (eventStreamConnection *EventStreamConnection) cleanUp() {
	close(eventStreamConnection.msgs)
}

func (eventStreamConnection *EventStreamConnection) sendEventInternal(event *Event) error {
	if event == nil || len(event.GetData()) == 0 {
		return errors.New("Event and data field is mandatory.")
	}

	if event.GetId() != nil && len(*event.GetId()) > 0 {
		_, err := fmt.Fprintf(eventStreamConnection.responseWriter, "id: %s\n", *event.GetId())
		if err != nil {
			return err
		}
	}

	if event.GetEvent() != nil && len(*event.GetEvent()) > 0 {
		_, err := fmt.Fprintf(eventStreamConnection.responseWriter, "event: %s\n", *event.GetEvent())
		if err != nil {
			return err
		}
	}

	//According to spec http://www.w3.org/TR/2015/REC-eventsource-20150203/
	//every line in data should have a new data: entry
	dataEntries := regexp.MustCompile("\r\n|\n\r|\n|\r").Split(event.GetData(), -1)

	for _, entry := range dataEntries {
		_, err := fmt.Fprintf(eventStreamConnection.responseWriter, "data: %s\n", entry)
		if err != nil {
			return err
		}
	}

	if event.GetRetry() != nil {
		_, err := fmt.Fprintf(eventStreamConnection.responseWriter, "retry: %d\n", *event.GetRetry())
		if err != nil {
			return err
		}
	}

	//According to spec http://www.w3.org/TR/2015/REC-eventsource-20150203/
	//An empty line either [CRLF or CR or LF] is used to delimit events.
	_, err := fmt.Fprintf(eventStreamConnection.responseWriter, "\r\n")
	if err != nil {
		return err
	}

	flusher := eventStreamConnection.responseWriter.(http.Flusher)
	flusher.Flush()

	return nil
}

func (eventStreamConnection *EventStreamConnection) SendEvent(event *Event) (err error) {
	if event == nil {
		return errors.New("Event is nil")
	}
	err = event.Validate()

	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {

			err = errors.New("This stream is closed.")
		}
	}()

	select {
	case eventStreamConnection.msgs <- event:
	default:
		return errors.New("Error sending event. Buffer full")
	}
	return nil
}

func (eventStreamConnection *EventStreamConnection) GetLastEventId() *string {
	if len(eventStreamConnection.lastEventId) == 0 {
		return nil
	}

	return &eventStreamConnection.lastEventId
}

func (eventStreamConnection *EventStreamConnection) CloseNotify() chan bool {
	return eventStreamConnection.closeNotify
}
