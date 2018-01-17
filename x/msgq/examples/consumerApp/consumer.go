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
	"fmt"
	"os"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
	"strconv"
	"time"
)

const (
	TestBatchSize = 50
	DEF_NUM_MSG   = 10
	DEF_TOPIC     = "testtopic"
)

type userConf struct {
	numMsg int
	topics string
}

func readConfEnv() (userConf, error) {
	//Create config with default values
	var err error
	var userConfigs = userConf{DEF_NUM_MSG, DEF_TOPIC}

	numMsg := os.Getenv("NUM_MSG")
	userConfigs.numMsg, err = strconv.Atoi(numMsg)

	if numMsg == "" || userConfigs.numMsg < 0 {
		mlog.Emit(mlog.INFO, fmt.Sprintf("Number of messages is negative. Kindly proide positive value. Setting default value-10 for now"))
		userConfigs.numMsg = DEF_NUM_MSG
		err = nil
	}

	userConfigs.topics = os.Getenv("TOPIC")

	if userConfigs.topics == "" {
		mlog.Emit(mlog.INFO, fmt.Sprintf("Topic is null. Kindly proide a topic name. Setting default value-testtopic for now"))
		userConfigs.topics = DEF_TOPIC
		err = nil
	}

	return userConfigs, err
}

func createMsgQnConf(msgQ msgq.MessageQueue, topicConf userConf) (msgq.MsgQConsumer, error) {

	consConfig := &msgq.ConsumerConfig{TopicName: topicConf.topics}

	cons, err := msgQ.NewConsumer(consConfig)
	if err != nil {
		mlog.Emit(mlog.ERROR, fmt.Sprintf("Failed to create consumer :%v", err))
		return nil, err
	}

	mlog.Emit(mlog.INFO, fmt.Sprintf("Configuration for msgQ: %s is :%v", topicConf.topics, cons))
	return cons, err
}

func main() {

	//msgCreateAndPrint()
	// Read all env configs
	userConfigs, err := readConfEnv()

	mlog.Emit(mlog.INFO, fmt.Sprintf("Configuration read from env var is :%v", userConfigs))

	msgQ, err := msgq.NewMsgQ("safService", nil, nil)

	if err != nil {
		mlog.Emit(mlog.ERROR, fmt.Sprintf("msgq creation failed due to :%v", err))
		return
	}
	mlog.Emit(mlog.INFO, fmt.Sprintf("msgQ is:%v", msgQ))

	msgQcons, err := createMsgQnConf(msgQ, userConfigs)

	if err != nil {
		mlog.Emit(mlog.ERROR, fmt.Sprintf("msgq creation failed due to :%v", err))
	}

	msgCount := 0
	msgCntIn1Tick := 0
	tick := time.Tick(60 * time.Second)

	for {
		select {
		case event := <-msgQcons.Receive():
			if event.Err != nil {
				mlog.Emit(mlog.ERROR, fmt.Sprintf("msgq received failed due to :%v", event.Err))
			}
			//fmt.Println(string(event.Value), event.Offset)
			err = msgQcons.SetAckOffset(event.Offset)
			if err != nil {
				mlog.Emit(mlog.ERROR, fmt.Sprintf("Error writing :%v", err))

			}
			msgCount++

			if msgCount%777777 == 0 {
				fmt.Println(string(event.Value), event.Offset)
			}

		case <-tick:
			if (msgCntIn1Tick != 0) && ((msgCount - msgCntIn1Tick) == 0) {
				mlog.Info("No Message recieved since last one minute")
			}
			msgCntIn1Tick = msgCount
		}
	}
	mlog.Emit(mlog.INFO, fmt.Sprintf("Received total msgs :%d", msgCount))

	//time.Sleep(1500 * time.Second)
}
