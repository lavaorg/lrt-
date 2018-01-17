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
	"container/list"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/verizonlabs/northstar/pkg/crypto"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/zklib"
	"github.com/verizonlabs/northstar/pkg/zklib/config"
	"github.com/verizonlabs/northstar/pkg/vaultlib"
)

func (cons *Consumer) Receive() chan *ConsumerEvent {
	return cons.ReceiveChan
}

func (cons *Consumer) DecryptMsg(msg []byte) ([]byte, error) {
	if cons.Encryption == false {
		return msg, nil
	}

	key := vaultlib.GetAes256Key()
	nonce := make([]byte, 12)
	copy(nonce[:4], cons.TopicName[:4])
	copy(nonce[4:], msg[:8])

	plainText, err := crypto.Decrypt(msg[8:], nonce, key)
	if err != nil {
		mlog.Error("Failed to decrypt the message %v", err)
		return nil, err
	}

	return plainText, nil
}

func (cons *Consumer) ProcessReceivedMessage(ok bool, msg *sarama.ConsumerMessage) bool {
	if !ok {
		mlog.Info("Message channel was closed by Sarama")
		return true
	}

	m, err := cons.DecryptMsg(msg.Value)
	if err != nil {
		mlog.Error("Failed to decrypt the message", err)
		return false
	}

	evt := ConsumerEvent{
		Key:       msg.Key,
		Value:     m,
		Offset:    msg.Offset,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Err:       nil,
	}

	cons.insertOffset(int64(msg.Offset))

	cons.NumMsgsReceivedCounter.Incr()

	cons.ReceiveChan <- &evt

	return false
}

// insert the message offset in the offsetData struct
func (cons *Consumer) insertOffset(offset int64) {
	cons.OffsetData.Lock()
	e := cons.OffsetData.OffsetList.PushBack(offset)
	cons.OffsetData.OffsetMap[offset] = e
	mlog.Debug("Inserted offset %v in the offsetMap and offsetList", offset)
	cons.OffsetData.Unlock()
}

func (cons *Consumer) Close() error {
	atomic.AddInt32(&cons.ReqForClose, 1)
	return cons.SaramaConsumer.Close()
}

func (cons *Consumer) ProcessReceivedError(ok bool, msg *sarama.ConsumerError) bool {
	if !ok {
		mlog.Alarm("Offset out of range. Possibly, log segments are getting deleted before consumer could read. Kindly, check and tune your kafka log retention policy")
		return true
	}

	evt := ConsumerEvent{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Err:       msg.Err,
	}
	// increment the stats counter of number of error messages
	cons.NumRecvErrCounter.Incr()

	cons.ReceiveChan <- &evt
	return false
}

//This routine waits for the errors or messages from Sarama
//and writes the same to a channel that the applications must read from.
//When Sarama encounters Invalid offset error, Sarama closes the message
//and error channels which will be handled by this routine to restart the
//consumer with Oldest known offset.
//The function exits with a panic if it fails to create a new consumer with
//oldest offset.
func (cons *Consumer) ReceiveRoutine() {
	err_closed := false
	msg_closed := false

	for {
		select {
		case msg, ok := <-cons.SaramaConsumer.Errors():
			err_closed = cons.ProcessReceivedError(ok, msg)

		case msg, ok := <-cons.SaramaConsumer.Messages():
			msg_closed = cons.ProcessReceivedMessage(ok, msg)
		}

		if err_closed || msg_closed {
			break
		}
	}

	//Drain the channels for the old consumer object
	if err_closed {
		for {
			msg, ok := <-cons.SaramaConsumer.Messages()
			if cons.ProcessReceivedMessage(ok, msg) {
				break
			}
		}
		mlog.Info("Consumer ErrorChannel was closed, processed all message")
	}

	//Drain the channels for the old consumer object
	if msg_closed {
		for {
			msg, ok := <-cons.SaramaConsumer.Errors()
			if cons.ProcessReceivedError(ok, msg) {
				break
			}
		}
		mlog.Info("Consumer MessageChannel was closed, processed all Errmessage")
	}

	if atomic.LoadInt32(&cons.ReqForClose) == 0 {
		var err error

		numRetries := 0
		retryInterval := config.RetryStartInterval
		for {
			cons.SaramaConsumer, err = cons.BaseConsumer.ConsumePartition(
				cons.TopicName,
				cons.PartitionNum,
				sarama.OffsetOldest)

			if err != nil {
				if (numRetries < config.NumRetries) && isConnError(err) {
					waitRetryBackoff(&numRetries, &retryInterval, "Error ConsumePartition in Kafka", err)
				} else {
					mlog.Error("Failed to create consumer for %v:%d. Err %v", cons.TopicName, cons.PartitionNum, err)
					panic("Failed to create the consumer")
				}
			} else {
				mlog.Info("Created new consumer after getting offset out of range")
				break
			}
		}

		go cons.ReceiveRoutine()
	}
}

func (cons *Consumer) OffsetCommitRoutine() {
	lastOffset := atomic.LoadInt64(&cons.CommitOffset)
	timer := time.NewTicker(time.Second * 1)
	for {
		<-timer.C
		//Timer expired
		offsetVal := atomic.LoadInt64(&cons.CommitOffset)
		if offsetVal == lastOffset {
			// Check the oldest offset available on the broker.
			// If the least offset currently in the offset list is less than the oldest offset
			// available on broker, remove all the offsets in
			// the offset map and list that are less than the
			// oldest offset available on broker
			offsetOldest, err := cons.getOldestOffsetOnBroker()
			if err != nil {
				mlog.Error("Failed to get oldest offset from Kafka broker %v", err)
				continue
			}
			mlog.Debug("Oldest offset available on broker %v", offsetOldest)
			mlog.Debug("Least offset ie first element of offset list %v", offsetVal)
			if offsetVal >= offsetOldest {
				continue
			} else {
				var next *list.Element
				cons.OffsetData.Lock()
				for e := cons.OffsetData.OffsetList.Front(); e != nil; e = next {
					next = e.Next()
					if e.Value.(int64) == offsetOldest {
						break
					} else {
						mlog.Debug("Removing offset %v from the offsetMap and offsetList", e.Value.(int64))
						delete(cons.OffsetData.OffsetMap, e.Value.(int64))
						cons.OffsetData.OffsetList.Remove(e)
					}
				}
				cons.OffsetData.Unlock()
				mlog.Debug("NOW the least offset is %v", offsetOldest)
				offsetVal = offsetOldest
				atomic.StoreInt64(&cons.CommitOffset, offsetOldest)
			}
		}

		err := CommitOffset(cons.ZkCon, cons.OffsetInfoZkPath, offsetVal)
		if err != nil {
			mlog.Error("Failed to write commit offset to zookeeper %v", err)
			cons.NumZkCommitFailCounter.Incr()
		} else {
			mlog.Debug("Commited offset %d", offsetVal)
			lastOffset = offsetVal
		}
	}
}

// return the oldest offset available on the broker
func (cons *Consumer) getOldestOffsetOnBroker() (int64, error) {
	oldestoffset, err := cons.msgqRef.Client.GetOffset(cons.TopicName, cons.PartitionNum, sarama.OffsetOldest)
	if err != nil {
		mlog.Error("Error while getting oldest offset %v", err)
		return 0, err
	}
	return oldestoffset, nil
}

//Application marks the message as processed by removing it from the
//List
func (cons *Consumer) SetAckOffset(offset int64) (err error) {
	cons.OffsetData.Lock()
	defer cons.OffsetData.Unlock()

	// increment the stats counter of number of messages succesfully processed by app to zookeeper
	cons.NumMsgsCommitedCounter.Incr()

	e := cons.OffsetData.OffsetList.Front()
	if e == nil {
		return nil
	}

	atomic.StoreInt64(&cons.CommitOffset, e.Value.(int64))
	mlog.Debug("Least offset %v", cons.CommitOffset)

	// find the setAckOffset and remove it from the list and map
	if element, prs := cons.OffsetData.OffsetMap[offset]; prs {
		mlog.Debug("Found offset %v in the offsetMap", offset)
		delete(cons.OffsetData.OffsetMap, offset)
		cons.OffsetData.OffsetList.Remove(element)
		mlog.Debug("Deleted offset %v from the offsetMap and offsetList", offset)
	} else {
		mlog.Debug("Could not find offset %v in the offsetMap", offset)
		cons.ErrOffsetNotFound.Incr()
	}

	return nil
}

// this API will get Ack offset in the zookeeper based on the ack
// the purpose of setting this AckOffset is to restart the library from given offset
func (cons *Consumer) GetOffset() (offset int64, err error) {
	// if offset corresponding to this partition exists, get the offset
	val, _, err := cons.ZkCon.GetData(cons.OffsetInfoZkPath)
	if err == zklib.ErrNoNode {
		return 0, nil
	} else if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(val), 10, 64)
}

func (cons *Consumer) GetPartitionNum() int32 {
	return cons.PartitionNum
}

func (cons *Consumer) UpdateOffsetStats(interval int) {
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for {
		select {
		case <-ticker.C:
			zkpOffset, _ := cons.GetOffset()
			cons.OffsetZkp.Set(zkpOffset)
			cons.OffsetKafka.Set(cons.SaramaConsumer.HighWaterMarkOffset())
		}
	}
}
