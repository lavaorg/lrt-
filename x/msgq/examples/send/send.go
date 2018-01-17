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
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
	//"encoding/binary"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
	"github.com/verizonlabs/northstar/pkg/msgs"
	//"github.com/verizonlabs/northstar/pkg/external/utils"
	"github.com/verizonlabs/northstar/pkg/sdscodec"
	//"github.com/verizonlabs/northstar/pkg/servdisc"
	"github.com/verizonlabs/northstar/pkg/msgq/topics"
)

func main() {

	msgQ, err := msgq.NewMsgQ("Utag", nil, nil)
	if err != nil {
		fmt.Errorf("msgq creation failed ")
		return
	}

	// create producer for desired topic
	prodConfig := &msgq.ProducerConfig{
		TopicName:   topics.UserToken,
		Partitioner: msgq.RandomPartitioner,
	}
	prod, err := msgQ.NewProducer(prodConfig)
	if err != nil {
		fmt.Errorf("Failed to create producer. %v", err)
		return
	}

	// create the request message to be sent to the service

	// Token Service
	//gob.Register(msgs.TokenServiceRequest{})
	//gob.Register(msgs.TokenServiceRequestSleep{})
	gob.Register(msgs.TokenServiceRequestKIll{})
	tsr := mockTSReq()

	// External Service
	//gob.Register(msgs.ExternalDataMsg{})
	//tsr := MockExtReq()

	// Utsched
	//gob.Register(msgs.UTSchedulerReq{})
	//tsr := MockSchedReq()

	// UtPosition
	//gob.Register(msgs.PositionMessage{})
	//gob.Register(msgs.PositionMsgStd{})
	//gob.Register(msgs.PositionMsgNonStd{})
	//tsr := MockUtPosReq()

	mlog.Info("XXXXXXX Payload %+v", tsr)

	// Encode payload
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(tsr); err != nil {
		fmt.Errorf("error in encoding")
	}

	// specify rate at which message is to be sent
	// TODO Make rate and duration cmd line paramaters
	//rate := 1
	//duration := 1
	//sendMsgAtGivenFreq(rate, duration, prod, buf.Bytes())

	sendErr := prod.Send(buf.Bytes())
	if sendErr != nil {
		mlog.Error("Error sending message %v", sendErr)
		return
	}
	mlog.Info("Successfully sent message")
	time.Sleep(2 * time.Second)

}

// Send messages at the given rate
//func sendMsgAtGivenFreq(rate int, duration int, prod msgq.MsgQProducer, msg []byte) {
//	count := 0
//	total_messages := rate * duration
//	num := int(time.Second / time.Microsecond) // represents "1000000"
//	td := (num / rate)
//	tick := time.NewTicker(time.Microsecond * time.Duration(td))
//	defer tick.Stop()

//	for {
//		select {
//		case <-tick.C:
//			sendErr := prod.Send(msg)
//			if sendErr != nil {
//				mlog.Error("Error sending message %v", sendErr)
//				return
//			}
//			count += 1
//			mlog.Info("Sent %v of %v  messages", count, total_messages)
//			if count == total_messages {
//				mlog.Info("Sent %v messages @ %v messages per second", total_messages, rate)
//				return
//			}
//		}
//	}
//}

// Methods to create mock message types

// Token message
func mockTSReq() *msgs.TokenService {
	tsmsg := msgs.TokenServiceRequestKIll{
		Action:        msgs.ACTION_KILL,
		KillOnPowerOn: false,
	}

	return &msgs.TokenService{
		Header: msgs.MsgHeader{
			Imsi:  20200000000003,
			MsgId: "testMsgId",
		},
		MessageType:       sdscodec.FwdMsgDisableTag,
		DataCenterID:      1,
		CallbackURL:       "http://verizon.com",
		ReportingInterval: 10,
		ScheduledTime:     time.Now(),
		RequestMsg:        &tsmsg,
	}
}

// Utsched Request message
//func MockSchedReq() *msgs.UTSchedulerReq {

//    return &msgs.UTSchedulerReq{
//        Header: msgs.MsgHeader{
//            Imsi: 5,
//			OriginService: servdisc.TOKEN_SERVICE,
//        },
//        MsgType:  sdscodec.FwdMsgLocationData,
//        Priority: msgs.UTSCHED_AT,
//        ScheduledTime: time.Now().Add(time.Second*60),
//		LastInbound: time.Now(),
//		LastOutbound: time.Now(),
//		ReportingInterval: 300,
//    }
//}

// UtPosition Message
//func MockUtPosReq() *msgs.PositionMessage {
//	msg := msgs.PositionMsgStd{
//		MsgsReceived: [64]byte{1},
//	}

//	return &msgs.PositionMessage{
//		Header: msgs.MsgHeader{
//			Imsi:  5,
//			MsgId: "testMsgId",
//		},
//		Source:  msgs.SDS_MSG,
//		MsgType: sdscodec.RetMsgStandard,
//		CellId:  1234,
//		Msg:     &msg,
//		Imei:    "11111",
//		Color:   "black",
//	}
//}

// External Message
//func MockExtReq() *msgs.ExternalDataMsg {

//	var data [210]byte
//	msg := utils.ResponsePayload{
//		Header:  utils.RetMsgHeader{
//			TypeAndFlag: 0x10,
//		},
//		InResponseTo: 0,
//		DataLength:   uint8(len(data)),
//		Data:         data,
//	}

//	buf := new(bytes.Buffer)
//	err := binary.Write(buf, binary.LittleEndian, msg)
//	if err != nil {
//		mlog.Error("binary.Write failed : %s", err)
//	}

//	var result [215]byte
//	copy(result[:], buf.Bytes()[:])

//	return &msgs.ExternalDataMsg {
//		Header:    msgs.MsgHeader{
//			Imsi:  1,
//		},
//		MsgType:  sdscodec.RetMsgExternalData,
//		Data:     result,
//	}
//}
