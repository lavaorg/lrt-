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
	"strings"
	"sync"
	"time"

	_ "github.com/Shopify/sarama"
)

const (
	TestBatchSize      = 50
	DEF_NUM_MSG        = 10
	DEF_TOPIC          = "testtopic"
	DEF_PARTITIONER    = msgq.RoundRobinPartitioner
	DEF_SIZE_MSG       = 100
	DEF_RATE_CTRL_WAIT = 1
)

type prodConf struct {
	numMsg       int
	topics       string
	partitioner  msgq.PartitionerType
	sizeMsg      int
	rateCtrlWait int
}

//var status = msgq.UP
var status int = 0
var stL sync.Mutex

func readConfEnv() (prodConf, error) {
	//Create config with default values
	var err error
	var prodConfigs = prodConf{DEF_NUM_MSG, DEF_TOPIC, DEF_PARTITIONER, DEF_SIZE_MSG, DEF_RATE_CTRL_WAIT}

	numMsg := os.Getenv("NUM_MSG")
	prodConfigs.numMsg, err = strconv.Atoi(numMsg)

	if prodConfigs.numMsg < 0 {
		mlog.Emit(mlog.INFO, fmt.Sprintf("Number of messages is negative. Kindly proide positive value. Setting default value-10 for now"))
		prodConfigs.numMsg = DEF_NUM_MSG
		err = nil
	}

	prodConfigs.topics = os.Getenv("TOPIC")

	if prodConfigs.topics == "" {
		mlog.Emit(mlog.INFO, fmt.Sprintf("Topic is null. Kindly proide a topic name. Setting default value-testtopic for now"))
		prodConfigs.topics = DEF_TOPIC
		err = nil
	}
	// read the partitioner type to use with
	partitionerType := os.Getenv("PARTITIONER")

	if partitionerType == "" {
		mlog.Emit(mlog.INFO, fmt.Sprintf("partitioner is not provided from env. Kindly proide a partitioner type. Setting default value-roundrobin for now"))
		prodConfigs.partitioner = DEF_PARTITIONER
		err = nil
	} else {
		if partitionerType == "hash" {
			prodConfigs.partitioner = msgq.HashPartitioner
		} else if partitionerType == "manual" {
			prodConfigs.partitioner = msgq.ManualPartitioner
		} else if partitionerType == "random" {
			prodConfigs.partitioner = msgq.RandomPartitioner
		} else {
			prodConfigs.partitioner = msgq.RoundRobinPartitioner
		}
	}

	sizeMsg := os.Getenv("SIZE_MSG")
	prodConfigs.sizeMsg, err = strconv.Atoi(sizeMsg)

	if prodConfigs.sizeMsg < 0 {
		mlog.Emit(mlog.INFO, fmt.Sprintf("Size of messages is negative. Kindly proide positive value. Setting default value-100 for now"))
		prodConfigs.numMsg = DEF_SIZE_MSG
		err = nil
	}

	rateCtrlWait := os.Getenv("RATE_CTRL_WAIT")
	prodConfigs.rateCtrlWait, err = strconv.Atoi(rateCtrlWait)

	if prodConfigs.rateCtrlWait < 0 {
		mlog.Emit(mlog.INFO, fmt.Sprintf("Rate control is negative. Kindly proide positive value. Setting default value-1 nanosecond for now"))
		prodConfigs.rateCtrlWait = DEF_RATE_CTRL_WAIT
		err = nil
	}

	return prodConfigs, err
}

func prepareMsgQ(msgQ msgq.MessageQueue, prodConfig *msgq.ProducerConfig) (msgq.MsgQProducer, error) {

	mlog.Emit(mlog.INFO, fmt.Sprintf("producer config  %v", prodConfig))

	prod, err := msgQ.NewProducer(prodConfig)

	if err != nil {
		mlog.Emit(mlog.ERROR, fmt.Sprintf("Failed to create producer: %v", err))
		return nil, nil
	}

	return prod, err
}

/* Prepare message as per size of the message read from environment variable */
func prepareMsg(userConfig *prodConf) string {
	var message string
	var partitioner string

	switch {
	case userConfig.partitioner == msgq.HashPartitioner:
		partitioner = "Hash Partitioner "
	case userConfig.partitioner == msgq.ManualPartitioner:
		partitioner = "Manual Partitioner "
	case userConfig.partitioner == msgq.RandomPartitioner:
		partitioner = "Random Partitioner "
	default:
		partitioner = "Round Robin Partitioner "
	}
	/* Initial message size */
	message = partitioner + ": %d "
	mlog.Emit(mlog.INFO, fmt.Sprintf("initial message: %d", len(message)))
	/* Calculate remaining size of the message */
	remMsg := userConfig.sizeMsg - len(message)
	numRepeat := remMsg / len(partitioner)
	mlog.Emit(mlog.INFO, fmt.Sprintf("1:rem message: %d and numRepeat: %d", remMsg, numRepeat))
	message = message + strings.Repeat(partitioner, numRepeat)
	/* Calculate remaining size of the message */
	remMsg = userConfig.sizeMsg - len(message)
	mlog.Emit(mlog.INFO, fmt.Sprintf("2:rem message: %d", remMsg))
	message = message + strings.Repeat("E", remMsg)

	mlog.Emit(mlog.INFO, fmt.Sprintf("constructed message: %s", message))

	return message
}

func main() {

	//var wg sync.WaitGroup
	// start reading configuration of producer from env var
	userConfig, err := readConfEnv()

	mlog.Emit(mlog.INFO, fmt.Sprintf("Configurstions from env read are :%v", userConfig))

	//msgQ, err := msgq.NewMsgQ("testService", []string {"10.1.12.1:8092","10.1.35.1:8092","10.1.13.1:8092"}, []string {"10.1.12.1:6191","10.1.35.1:6191","10.1.13.1:6191"})
	//msgQ, err := msgq.NewMsgQ("testService", []string {"10.1.12.1:8099"}, []string {"10.1.12.1:6199"})

	//msgQ, err := msgq.NewMsgQ("testService", []string {"10.1.12.1:8092,10.1.35.1:8092,10.1.13.1:8092"}, []string {"10.1.12.1:6191,10.1.35.1:6191,10.1.13.1:6191"})
	msgQ, err := msgq.NewMsgQ("testService", nil, nil)
	if err != nil {
		mlog.Emit(mlog.INFO, fmt.Sprintf("msgq creation failed"))
		return
	}
	//msgq.MaxBufferTime = 2000
	//msgq.MaxBufferedBytes = 1024 * 2

	/*prodConfig := &msgq.ProducerConfig{
		TopicName:   userConfig.topics + "RR",
		Partitioner: msgq.RoundRobinPartitioner,
	}*/

	prod, err := prepareMsgQ(msgQ, &msgq.ProducerConfig{TopicName: userConfig.topics, Partitioner: userConfig.partitioner})
	go listenErrorAndTopAvailChann(msgQ, prod)
	/* Prepare message as per size of the message read from environment variable */
	message := prepareMsg(&userConfig)

	switch {
	case userConfig.partitioner == msgq.HashPartitioner:
		goto hash
	case userConfig.partitioner == msgq.ManualPartitioner:
		goto manual
	case userConfig.partitioner == msgq.RandomPartitioner:
		goto random
	default:
		goto roundrobin
	}

roundrobin:
	for i := 0; i < userConfig.numMsg; i++ {
		stL.Lock()
		st := status
		stL.Unlock()
		//if (msgq.UP == st) {
		if 0 == st {
			err = prod.Send([]byte(fmt.Sprintf(message, i)))
			if err != nil {
				mlog.Emit(mlog.INFO, fmt.Sprintf("msgq Send failed, trying to resend the message again"))
				err := prod.Send([]byte(fmt.Sprintf("testing SAF RAND:%d", i)))
				if err != nil {
					mlog.Emit(mlog.ERROR, fmt.Sprintf("resending also failed failed"))
				} else {
					mlog.Emit(mlog.INFO, fmt.Sprintf("resending PASS"))
				}
			}
			//time.Sleep(100 * time.Millisecond)
		} else {
			//time.Sleep(1 * time.Second)
			i--
		}
	}
	goto exit

random:
	for i := 0; i < userConfig.numMsg; i++ {
		err = prod.Send([]byte(fmt.Sprintf(message, i)))
		if err != nil {
			mlog.Emit(mlog.INFO, fmt.Sprintf("msgq Send failed, trying to resend the message again"))
			err = prod.Send([]byte(fmt.Sprintf(message, i)))
			if err != nil {
				mlog.Emit(mlog.ERROR, fmt.Sprintf("resending also failed failed"))
			} else {
				mlog.Emit(mlog.INFO, fmt.Sprintf("resending PASS"))
			}
		}
	}
	goto exit

hash:
	for i := 0; i < userConfig.numMsg; i++ {
		err = prod.SendKeyMsg([]byte(fmt.Sprintf("SAF Hash:%d", i%2)), []byte(fmt.Sprintf(message, i)))
		if err != nil {
			mlog.Emit(mlog.INFO, fmt.Sprintf("msgq Send failed, trying to resend the message again"))
			err = prod.SendKeyMsg([]byte(fmt.Sprintf("SAF Hash:%d", i%2)), []byte(fmt.Sprintf(message, i)))
			if err != nil {
				mlog.Emit(mlog.ERROR, fmt.Sprintf("resending also failed failed"))
			} else {
				mlog.Emit(mlog.INFO, fmt.Sprintf("resending PASS"))
			}
		}
	}
	goto exit

manual:
	for i := 0; i < userConfig.numMsg; i++ {
		err = prod.SendToPartition(0, []byte(fmt.Sprintf(message, i)))
		if err != nil {
			mlog.Emit(mlog.INFO, fmt.Sprintf("msgq Send failed, trying to resend the message again"))
			err = prod.Send([]byte(fmt.Sprintf(message, i)))
			if err != nil {
				mlog.Emit(mlog.ERROR, fmt.Sprintf("resending also failed failed"))
			} else {
				mlog.Emit(mlog.INFO, fmt.Sprintf("resending PASS"))
			}
		}
	}
	goto exit

exit:
	time.Sleep(time.Second * time.Duration(userConfig.numMsg))
	mlog.Emit(mlog.INFO, fmt.Sprintf("producer done"))
	return
}

func listenErrorAndTopAvailChann(msgQ msgq.MessageQueue, prod msgq.MsgQProducer) {
	for {
		select {
		case event := <-msgQ.ReceiveTopicAvailMonEvt():
			mlog.Info("listenErrorAndTopAvailChann: Avail Channel: ", event)
			stL.Lock()
			//status = event.TopicStatus
			if msgq.NOTIF_DOWN == event.EventType {
				//status++
			} else {
				//status--
			}
			stL.Unlock()
			//fmt.Println(string(event.Value), event.Offset)
		case event := <-prod.ReceiveErrors():
			mlog.Info("listenErrorAndTopAvailChann: Error channel: ", event)
		}
	}
}
