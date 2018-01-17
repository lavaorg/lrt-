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

package main

import (
	"github.com/verizonlabs/northstar/pkg/ekk"
	"github.com/verizonlabs/northstar/pkg/ekk/config"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
	"github.com/verizonlabs/northstar/pkg/service_master"
)

type (
	worker struct {
		message  []byte
		offset   int64
		consumer msgq.MsgQConsumer
	}
)

var (
	kafka         *ekk.Kafka
	elasticsearch *ekk.Elasticsearch
	err           error
)

func newWorker(e *msgq.ConsumerEvent, c msgq.MsgQConsumer) *worker {
	return &worker{
		message:  e.Value,
		offset:   e.Offset,
		consumer: c,
	}
}

func (w *worker) Run(n int) (err error) {
	mlog.Debug("run(), worker: %d", n)
	if err = elasticsearch.WriteLog(w.message); err != nil {
		return
	}
	return w.consumer.SetAckOffset(w.offset)
}

func main() {
	sm := service_master.New(config.NumWorkers, config.WorkQueueSize)
	kafka, err = ekk.NewKafka()
	if err != nil {
		mlog.Error("error creating kafka client")
	}
	elasticsearch, err = ekk.NewElasticsearch()
	if err != nil {
		mlog.Error("error creating elasticsearch client")
	}

	for {
		select {
		case evt := <-kafka.StderrConsumer.Receive():
			if evt.Err != nil {
				mlog.Error(evt.Err.Error())
				continue
			}
			w := newWorker(evt, kafka.StderrConsumer)
			if err := sm.Dispatch(config.ServiceName, w); err != nil {
				mlog.Error(err.Error())
			}
		case evt := <-kafka.StdoutConsumer.Receive():
			if evt.Err != nil {
				mlog.Error(evt.Err.Error())
				continue
			}
			w := newWorker(evt, kafka.StdoutConsumer)
			if err := sm.Dispatch(config.ServiceName, w); err != nil {
				mlog.Error(err.Error())
			}
		}
	}
}
