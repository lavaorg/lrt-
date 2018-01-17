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
	// "encoding/json"
	"fmt"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
	"time"
)

func main() {
	msgCreateAndPrint()
	time.Sleep(1500 * time.Second)
}

func msgCreateAndPrint() {
	msgQ, err := msgq.NewMsgQ("testService", nil, nil)
	if err != nil {
		mlog.Emit(mlog.ERROR, fmt.Sprintf("msgq creation failed due to :%v", err))
		return
	}
	mlog.Emit(mlog.INFO, fmt.Sprintf("msgQ is:%v", msgQ))

	consConfig := &msgq.ConsumerConfig{TopicName: "testtopic"}
	cons, err := msgQ.NewConsumer(consConfig)
	if err != nil {
		fmt.Println("Failed to create consumer %v", err)
	}

	msgCount := 0
consumerLoop:
	for {
		select {
		case event := <-cons.Receive():
			if event.Err != nil {
				mlog.Emit(mlog.ERROR, fmt.Sprintf("msgq received failed due to :%v", event.Err))
				fmt.Println("event has error")
			}
			fmt.Println(string(event.Value), event.Offset)
			err = cons.SetAckOffset(event.Offset)
			if err != nil {
				fmt.Println("Error writing %v", err)

			}
			msgCount++
		case <-time.After(1500 * time.Second):
			fmt.Println("> timed out")
			break consumerLoop
		}
	}
	mlog.Emit(mlog.INFO, fmt.Sprintf("Received msgs :%d", msgCount))

}
