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

package msgq

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
)

type MockMessageQueue struct{}

func (m MockMessageQueue) NewProducer(prodConf *msgq.ProducerConfig) (msgq.MsgQProducer, error) {
	mlog.Info("Creating new producer")
	return &MockProducer{}, nil
}

func (m MockMessageQueue) NewConsumer(consConf *msgq.ConsumerConfig) (msgq.MsgQConsumer, error) {
	mlog.Info("Creating new consumer")
	return &MockConsumer{}, nil
}

func (m MockMessageQueue) Partitions(topicName string) (int, error) {
	mlog.Info("Returning number of partitions")
	return 3, nil
}

func (m MockMessageQueue) ReceiveTopicAvailMonEvt() chan *msgq.TopicAvailEvent {
	mlog.Info("Returning channel for receiving available topics")
	return nil
}
