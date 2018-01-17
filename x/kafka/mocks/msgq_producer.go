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
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
)

type MockProducer struct{}

func (m MockProducer) Send(msg []byte) error {
	if string(msg) == "Simulate error message" {
		return fmt.Errorf("There was an error sending the message in the message queue")
	}

	mlog.Info("Sent message %s to nowhere", string(msg))
	return nil
}

func (m MockProducer) SendMsg(msg *sarama.ProducerMessage) error {
	mlog.Info("Producer topic %s sent nowhere", msg.Topic)
	return nil
}

func (m MockProducer) SendKeyMsg(key, msg []byte) error {
	mlog.Info("Producer sent message with key %v and message %v", key, msg)
	return nil
}

func (m MockProducer) SendToPartition(Partition int32, msg []byte) error {
	mlog.Info("Sending message %v to parition %v", msg, Partition)
	return nil
}

func (m MockProducer) ReceiveErrors() chan *msgq.ProducerEvent {
	mlog.Info("Recieving errors")
	return nil
}

func (m MockProducer) Close() error {
	mlog.Info("Closing the producer channel")
	return nil
}
