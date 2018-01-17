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

package ekk

import (
	"github.com/verizonlabs/northstar/pkg/ekk/config"
	"github.com/verizonlabs/northstar/pkg/msgq"
)

type (
	Kafka struct {
		queue          msgq.MessageQueue
		StderrConsumer msgq.MsgQConsumer
		StdoutConsumer msgq.MsgQConsumer
	}
)

func NewKafka() (*Kafka, error) {
	queue, err := msgq.NewMsgQ(config.ServiceName, nil, nil)
	if err != nil {
		return nil, err
	}
	stderrConsumer, err := queue.NewConsumer(&msgq.ConsumerConfig{
		TopicName: config.TopicStderr,
	})
	if err != nil {
		return nil, err
	}
	stdoutConsumer, err := queue.NewConsumer(&msgq.ConsumerConfig{
		TopicName: config.TopicStdout,
	})
	if err != nil {
		return nil, err
	}
	return &Kafka{
		queue:          queue,
		StderrConsumer: stderrConsumer,
		StdoutConsumer: stdoutConsumer,
	}, nil
}
