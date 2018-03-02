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
	"errors"
	"fmt"
	"testing"
)

type TestMsgq struct {
	ServiceName string
}

// Failure cases for Producer and Consumer
func (t TestMsgq) NewProducer(prodConf *ProducerConfig) (MsgQProducer, error) {
	err := errors.New("Test Producer error")
	return nil, err
}

func (t TestMsgq) NewConsumer(consConf *ConsumerConfig) (MsgQConsumer, error) {
	err := errors.New("Test Consumer error")
	return nil, err
}

// Success cases for Producer and Consumer
func (t TestMsgq) NewProducer2(prodConf *ProducerConfig) (MsgQProducer, error) {
	return nil, nil
}

func (t TestMsgq) NewConsumer2(consConf *ConsumerConfig) (MsgQConsumer, error) {
	return nil, nil
}

func TestNewProducerWithRetryFailure(t *testing.T) {
	t.Log("Start Test - TestNewProducerWithRetryFailure")

	testMsgq := TestMsgq{}
	testMsgq.ServiceName = "TestService"

	// create producer for desired topic
	prodConfig := &ProducerConfig{
		TopicName:   "TestTopic",
		Partitioner: RandomPartitioner,
	}

	_, err := testMsgq.NewProducerWithRetry(prodConfig)
	if err == nil {
		t.Errorf("Expected Error while creating producer. Instead got no error")
	}
	t.Log("Test Completed\n")
}

func (testmsgQ *TestMsgq) NewProducerWithRetry(prodConf *ProducerConfig) (MsgQProducer, error) {
	var retryInterval = 1
	var numRetries = 0
	// mock the operation succeeds
	var maxRetries = 10

	for {
		prod, err := testmsgQ.NewProducer(prodConf)
		if err != nil {
			if numRetries < maxRetries {
				errMsg := "Failed to create Msgq Producer. Retrying again..."
				waitRetryBackoff(&numRetries, &retryInterval, errMsg, err)
			} else {
				err = errors.New(fmt.Sprintf("Failed to create Msgq Producer even after %v max retries. Error:  %v", maxRetries, err.Error()))
				return nil, err
			}
		} else {
			return prod, nil
		}
	}

}

func TestNewProducerWithRetrySuccess(t *testing.T) {
	t.Log("Start Test - TestNewProducerWithRetrySuccess")

	testMsgq := TestMsgq{}
	testMsgq.ServiceName = "TestService"

	// create producer for desired topic
	prodConfig := &ProducerConfig{
		TopicName:   "TestTopic",
		Partitioner: RandomPartitioner,
	}

	_, err := testMsgq.NewProducerWithRetry2(prodConfig)
	if err != nil {
		t.Errorf("Expected no error while creating producer. Instead got error %v", err)
	}
	t.Log("Test Completed\n")
}

func (testmsgQ *TestMsgq) NewProducerWithRetry2(prodConf *ProducerConfig) (MsgQProducer, error) {
	var retryInterval = 1
	var numRetries = 0
	// mock the operation succeeds
	var maxRetries = 10

	for {
		_, err := testmsgQ.NewProducer2(prodConf)
		if err != nil {
			if numRetries < maxRetries {
				errMsg := "Failed to create Msgq Producer. Retrying again..."
				waitRetryBackoff(&numRetries, &retryInterval, errMsg, err)
			} else {
				err = errors.New(fmt.Sprintf("Failed to create Msgq Producer even after %v max retries. Error:  %v", maxRetries, err.Error()))
				return nil, err
			}
		} else {
			return nil, nil
		}
	}

}

func TestNewConsumerWithRetryFailure(t *testing.T) {
	t.Log("Start Test - TestNewConsumerWithRetryFailure")

	testMsgq := TestMsgq{}
	testMsgq.ServiceName = "TestService"

	// create consumer for desired topic
	consConfig := &ConsumerConfig{
		TopicName:  "TestTopic",
		Encryption: false,
	}

	_, err := testMsgq.NewConsumerWithRetry(consConfig)
	if err == nil {
		t.Errorf("Expected Error while creating consumer. Instead got no error")
	}
	t.Log("Test Completed\n")
}

func (testmsgQ *TestMsgq) NewConsumerWithRetry(consConfig *ConsumerConfig) (MsgQConsumer, error) {
	var retryInterval = 1
	var numRetries = 0
	// mock the operation succeeds
	var maxRetries = 10

	for {
		cons, err := testmsgQ.NewConsumer(consConfig)
		if err != nil {
			if numRetries < maxRetries {
				errMsg := "Failed to create Msgq Consumer. Retrying again..."
				waitRetryBackoff(&numRetries, &retryInterval, errMsg, err)
			} else {
				err = errors.New(fmt.Sprintf("Failed to create Msgq Consumer even after %v max retries. Error:  %v", maxRetries, err.Error()))
				return nil, err
			}
		} else {
			return cons, nil
		}
	}

}

func TestNewConsumerWithRetrySuccess(t *testing.T) {
	t.Log("Start Test - TestNewConsumerWithRetrySuccess")

	testMsgq := TestMsgq{}
	testMsgq.ServiceName = "TestService"

	// create consumer for desired topic
	consConfig := &ConsumerConfig{
		TopicName:  "TestTopic",
		Encryption: false,
	}

	_, err := testMsgq.NewConsumerWithRetry2(consConfig)
	if err != nil {
		t.Errorf("Expected no error while creating consumer. Instead got error %v", err)
	}
	t.Log("Test Completed\n")
}

func (testmsgQ *TestMsgq) NewConsumerWithRetry2(consConfig *ConsumerConfig) (MsgQConsumer, error) {
	var retryInterval = 1
	var numRetries = 0
	// mock the operation succeeds
	var maxRetries = 10

	for {
		_, err := testmsgQ.NewConsumer2(consConfig)
		if err != nil {
			if numRetries < maxRetries {
				errMsg := "Failed to create Msgq Consumer. Retrying again..."
				waitRetryBackoff(&numRetries, &retryInterval, errMsg, err)
			} else {
				err = errors.New(fmt.Sprintf("Failed to create Msgq Consumer even after %v max retries. Error:  %v", maxRetries, err.Error()))
				return nil, err
			}
		} else {
			return nil, nil
		}
	}

}
