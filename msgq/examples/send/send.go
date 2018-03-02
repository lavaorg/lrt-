package main

import (
	"fmt"
	"github.com/lavaorg/lrtx/mlog"
	"github.com/lavaorg/lrtx/msgq"
	"time"
)

func main() {

	msgQ, err := msgq.NewMsgQ("HelloQ", nil, nil)
	if err != nil {
		fmt.Errorf("msgq creation failed ")
		return
	}

	// create producer for desired topic
	prodConfig := &msgq.ProducerConfig{
		TopicName:   "HelloTopic",
		Partitioner: msgq.RandomPartitioner,
	}
	prod, err := msgQ.NewProducer(prodConfig)
	if err != nil {
		fmt.Errorf("Failed to create producer. %v", err)
		return
	}

	// create the request message to be sent to the service
	msg := "Hello"
	mlog.Info("XXXXXXX Payload %+v", msg)

	sendErr := prod.Send([]byte(msg))
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
