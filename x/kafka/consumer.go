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

package kafka

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
)

type ProcessMsg struct {
	Consumer msgq.MsgQConsumer
	Event    *msgq.ConsumerEvent
}

type receiveMsgCallback func(msg *ProcessMsg) error

type KafkaConsumer struct {
	msgQ msgq.MessageQueue
}

func NewKafkaConsumer(serviceName string) (*KafkaConsumer, error) {
	msgQ, err := msgq.NewMsgQ(serviceName, nil, nil)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{msgQ: msgQ}, nil
}

func (client *KafkaConsumer) ConsumeFromAllPartitions(topic string,
	callback receiveMsgCallback) error {
	nPartitions, err := client.msgQ.Partitions(topic)
	if err != nil {
		return err
	}
	mlog.Debug("Consuming data from %d partitions on topic: %s", nPartitions, topic)
	for i := 0; i < nPartitions; i++ {
		mlog.Debug("Creating consumer: %d", i)
		consumer, err := client.createConsumer(topic)
		if err != nil {
			return err
		}

		go client.readData(consumer, callback)
	}
	return nil
}

func (client *KafkaConsumer) ConsumeFromOnePartition(topic string,
	callback receiveMsgCallback) error {
	mlog.Debug("Consuming data from dedicated partition on topic: %s", topic)
	consumer, err := client.createConsumer(topic)
	if err != nil {
		return err
	}

	partition := consumer.GetPartitionNum()
	mlog.Debug("Consuming data from partition %v on topic: %s", partition, topic)
	go client.readData(consumer, callback)
	return nil
}

func (client *KafkaConsumer) createConsumer(topic string) (msgq.MsgQConsumer, error) {
	consumer, err := client.msgQ.NewConsumer(&msgq.ConsumerConfig{TopicName: topic})
	if err != nil {
		mlog.Error("Failed to create consumer %s with error %s ", topic, err.Error())
		return nil, err
	}
	return consumer, nil
}

func (client *KafkaConsumer) readData(consumer msgq.MsgQConsumer, callback receiveMsgCallback) {
	mlog.Debug("Consumer %d reading data", consumer.GetPartitionNum())

	for {
		select {
		case event := <-consumer.Receive():
			if event.Err != nil {
				mlog.Error("msgq received failed due to :%v", event.Err)
				continue
			}

			mlog.Debug("Data received")
			go callback(&ProcessMsg{Consumer: consumer, Event: event})
		}
	}
}
