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
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/vz/metadata"
	"github.com/samuel/go-zookeeper/zk"
	"hash"
	"hash/fnv"
	"math/rand"
	"github.com/verizonlabs/northstar/pkg/crypto"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq/config"
	"github.com/verizonlabs/northstar/pkg/vaultlib"
	"strconv"
	"time"
)

func GetProducerRef(msg *sarama.ProducerMessage) *Producer {
	prod := msg.Metadata.(*metadata.MessageMetadata).ProdRef.(*Producer)
	return prod
}

func GetProducerRefByTopic(prodList []*Producer, topic string) *Producer {
	for prod := range prodList {
		if prodList[prod].TopicName == topic {
			return prodList[prod]
		}
	}

	return nil
}

func (prod *Producer) Close() error {
	return prod.SaramaProducer.Close()
}
func (prod *Producer) GetAffectedPartList(partition int32) []int32 {
	list := []int32{}
	if metadata.INVALID_PARTITION == partition {
		// This is when whole topic are affected
		// Build the list of UP and non-blacklisted affected partitions
		for iThPart := range prod.PartitionSt {
			if (false == prod.CheckPartitionBlackListed(int32(iThPart))) && (DOWN != prod.GetPartitionStatus(int32(iThPart))) {
				list = append(list, int32(iThPart))
			}
		}
	} else {
		if (false == prod.CheckPartitionBlackListed(int32(partition))) && (DOWN != prod.GetPartitionStatus(partition)) {
			list = append(list, partition)
		}
	}

	return list
}

func (prod *Producer) GetPartitionStatus(partition int32) PartitionStatus {
	prod.partStatusLock.Lock()
	defer prod.partStatusLock.Unlock()

	return prod.PartitionSt[partition]
}

func (prod *Producer) GetTopicStatus() PartitionStatus {
	var topicStatus PartitionStatus = UP

	prod.partStatusLock.Lock()
	defer prod.partStatusLock.Unlock()

	for partition, partitionStatus := range prod.PartitionSt {
		mlog.Info("GetTopicStatus: Topic: ", prod.TopicName, "partition: ", partition, " and status: ", partitionStatus)
		if DOWN == partitionStatus {
			topicStatus = DOWN
			break
		}
	}

	return topicStatus
}

func (prod *Producer) SetPartitionStatus(partition int32, newStatus PartitionStatus) []PartitionStatus {

	prod.partStatusLock.Lock()
	defer prod.partStatusLock.Unlock()

	prod.PartitionSt[partition] = newStatus

	// Create a new slice to avoid false update as it may get updated before app process it
	copyPartSt := make([]PartitionStatus, len(prod.PartitionSt))
	copy(copyPartSt, prod.PartitionSt)
	return copyPartSt
}

func (prod *Producer) CheckPartitionBlackListed(partition int32) bool {
	//Check if choosen partition is in blacklist
	for i := range prod.ExcludeListPartitions {
		if prod.ExcludeListPartitions[i] == int(partition) {
			return true
		}
	}

	return false
}

func (prod *Producer) ReceiveErrors() chan *ProducerEvent {
	return prod.ReceiveChan
}

func (prod *Producer) ZkPathMonitorRoutine() {
	for {
		zkevt, ok := <-prod.ExcludeListPartitionChannel
		if ok && zkevt.Type == zk.EventNodeChildrenChanged {
			mlog.Info("Change in Children for %s. One or more partitions are blacklisted", prod.ExcludeListZkPath)
			//FIXME: Potential race condition as its updated and accessed across go routines
			ExcludeListPartitions, _, err := prod.ZkCon.Children(prod.ExcludeListZkPath)
			if err != nil {
				mlog.Error("Failed to get zk children for %s, err %v", prod.ExcludeListZkPath, err)
				continue
			}
			prod.ExcludeListPartitions = make([]int, len(ExcludeListPartitions))
			for i := range ExcludeListPartitions {
				val, err := strconv.Atoi(ExcludeListPartitions[i])

				if err != nil {
					mlog.Error("Failed to convert partition number to integer %v", err)
					break
				}

				prod.ExcludeListPartitions[i] = val
			}
		} else {
			//Handle the case of EventNotWatching and Unknown errors on channel
			mlog.Info("zkevent (%v) for %s. No partitions are blacklisted", zkevt, prod.ExcludeListZkPath)
			var err error
			_, prod.ExcludeListPartitionChannel, err = prod.ZkCon.GetChildrenWithWatchNCreate(prod.ExcludeListZkPath)
			if err != nil {
				mlog.Error("ExcludeListPartitionChannel are created with error: %v", err)
				break
			}
		}
	}
}

func (prod *Producer) WatchForErrors() {
	for {
		if errMsg, ok := <-prod.SaramaProducer.Errors(); ok == false {
			mlog.Info("Producer ErrorChannel closed for topic %s", prod.TopicName)
			break
		} else {
			if errMsg == nil {
				mlog.Error("Recieved empty error message for %s", prod.TopicName)
			} else {
				mlog.Error("Send failed with error %v", errMsg.Err)
			}
			// Increment the stats counter of total number of messages sent
			prod.NumMsgErrCounter.Incr()
			// send error to producer
			if prod.NotifyError && errMsg != nil {
				evt := ProducerEvent{
					Msg: errMsg.Msg,
					Err: errMsg.Err,
				}
				prod.ReceiveChan <- &evt
			}
		}
	}
}

func (prod *Producer) WatchForSuccess() {
	var numSuccessMsg uint64
	var msgDelay, msgNwRtDelay, totalMsgDelay, totalMsgNwRtDelay time.Duration

	for {
		if msg, ok := <-prod.SaramaProducer.Successes(); ok == false {
			mlog.Info("Producer SuccessChannel closed for topic %s", prod.TopicName)
			break
		} else {
			if msg == nil {
				mlog.Alarm("Recieved empty success message %s.. message metadata currupted", prod.TopicName)
				continue
			}
			numSuccessMsg++

			// Extract message delay = enqueue time and tranmission time of the message
			// Extract message network delay = TCP round trip delay of message (batch)
			msgDelay, msgNwRtDelay = msg.Metadata.(*metadata.MessageMetadata).GetMsgDelay()
			totalMsgDelay += msgDelay
			totalMsgNwRtDelay += msgNwRtDelay

			if numSuccessMsg < config.LatencySampleSize {
				// This is just to make sure some valid value intially when total number of sucessfull messages is less then number of Sample messages
				prod.AvgLatency.Set(float64(msgDelay))
				prod.AvgBatchNwLatency.Set(float64(msgNwRtDelay))
			}

			// Caluculate average as per configure number of samples
			if numSuccessMsg%config.LatencySampleSize == 0 {
				// Set the calculated values in to stats
				prod.AvgLatency.Set(float64(totalMsgDelay / time.Duration(config.LatencySampleSize)))
				prod.AvgBatchNwLatency.Set(float64(totalMsgNwRtDelay / time.Duration(config.LatencySampleSize)))
				// Reset total to start again a fresh total calculation
				totalMsgDelay, totalMsgNwRtDelay = 0, 0
			}
		}
	}
}

func (prod *Producer) EncryptMsg(msg []byte) ([]byte, error) {
	if prod.Encryption == false {
		return msg, nil
	}

	key := vaultlib.GetAes256Key()
	/* The nonce (12 byte) used for encrypting the messages consists
	   of a 8 byte counter and the first 4 bytes of the topic name.
	   The 8 byte counter is also sent along with the message in
	   plain-text (unencrypted).
	*/
	nonce := make([]byte, 12)
	//Need mutex??
	prod.NonceCounter += 1

	//Copy first 4 bytes of topic name to nonce
	copy(nonce[:4], prod.TopicName[:4])

	binary.PutUvarint(nonce[4:], prod.NonceCounter)

	encrypted, err := crypto.Encrypt(msg, nonce, key)
	if err != nil {
		mlog.Error("Failed to encrypt the messages %v", err)
		return nil, err
	}

	return append(nonce[4:], encrypted...), nil
}

func (prod *Producer) Send(msg []byte) error {

	msg, err := prod.EncryptMsg(msg)
	if err != nil {
		return err
	}

	prod.SaramaProducer.Input() <- &sarama.ProducerMessage{
		Topic:    prod.TopicName,
		Key:      nil,
		Value:    sarama.ByteEncoder(msg),
		Metadata: &metadata.MessageMetadata{EnqueuedAt: time.Now(), ProdRef: prod},
	}

	return nil
}

func (prod *Producer) SendMsg(msg *sarama.ProducerMessage) error {
	prod.SaramaProducer.Input() <- msg
	return nil
}
func (prod *Producer) SendKeyMsg(key, msg []byte) error {
	msg, err := prod.EncryptMsg(msg)
	if err != nil {
		return err
	}
	prod.SaramaProducer.Input() <- &sarama.ProducerMessage{
		Topic:    prod.TopicName,
		Key:      sarama.ByteEncoder(key),
		Value:    sarama.ByteEncoder(msg),
		Metadata: &metadata.MessageMetadata{EnqueuedAt: time.Now(), ProdRef: prod},
	}

	return nil
}

/* This function could be used to send a message to a given partition. However,
   this works only when the partitioner type is ManualPartitioner
*/
func (prod *Producer) SendToPartition(Partition int32, msg []byte) error {
	if prod.Partitioner != ManualPartitioner {
		mlog.Error("SendToPartition API can be used only with ManualPartitioner")
		return ErrInvalidPartitioner
	}

	if Partition < 0 {
		mlog.Error("Negative partition number passed")
		return ErrInvalidPartition
	}

	msg, err := prod.EncryptMsg(msg)
	if err != nil {
		return err
	}

	prod.SaramaProducer.Input() <- &sarama.ProducerMessage{
		Topic:     prod.TopicName,
		Key:       nil,
		Value:     sarama.ByteEncoder(msg),
		Partition: Partition,
		Metadata:  &metadata.MessageMetadata{EnqueuedAt: time.Now(), ProdRef: prod},
	}

	return nil
}

// Wrapper partitioner that is used by msgq library to support multiple
// producers per client
// THIS SHOULD NOT BE DIRECTLY USED BY THE APPS
type msgqPartitioner struct {
	hasher    hash.Hash32
	generator *rand.Rand
	partition int32
}

func newMsgQPartitioner(topic string) sarama.Partitioner {
	p := new(msgqPartitioner)
	p.hasher = fnv.New32a()
	p.generator = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	p.partition = 0
	return p
}

func incrementPartitionCounter(prod *Producer, chosenPartition, numPartitions int32) {
	//The prod.PartitionCounters array is created with the number of
	//partitions known at the time of creating the producer object.
	//So it will run into a index-out-of-range panic if new partitions
	//are added dynamically. Instead of checking for new partitions while
	//sending every message, it is optimal to handle it through a panic-recovery
	defer func() {
		oldPartLen := len(prod.PartitionCounters)
		if chosenPartition > numPartitions {
			// This is bug fix as a part of NPDDCS-2997-message-queue-app-crash-in-manual
			// This check avoid app to crash and let the message send to saram as sarama throw an error message
			mlog.Error("Send should fail with error kafka: partitioner returned an invalid partition index. Possibly all the partitions are blacklisted or topic have no partition")
		} else if int(numPartitions) > oldPartLen {
			mlog.Debug("Adding new stats counters for new partitions %v %v", numPartitions, oldPartLen)
			//New partitions are added. So add stats for the new added partitions
			prod.partStatusLock.Lock()
			for i := oldPartLen; i < int(numPartitions); i++ {
				mlog.Debug("Adding new partition counter for %v:%v", prod.TopicName, i)
				counter := statsObj.NewCounter("NumWrites_" + prod.TopicName + "_" + strconv.Itoa(i))
				prod.PartitionCounters = append(prod.PartitionCounters, counter)
				// Add a partition status
				prod.PartitionSt = append(prod.PartitionSt, UP)
			}
			prod.partStatusLock.Unlock()
			//For the partition for which we hit the panic, we need to increment the counter
			prod.PartitionCounters[chosenPartition].Incr()
		}
		if err := recover(); err != nil {
			mlog.Debug("Recovered in incrementPartitionCounter", err)
		}
	}()

	prod.PartitionCounters[chosenPartition].Incr()
}

func (p *msgqPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	prod := GetProducerRef(message)
	if prod == nil {
		mlog.Error("Message %v metadata does not contain producer reference. Failed to select partition for this message", message)
		return -1, ErrNoMetadata
	}
	var chosenPartition int32 = -1
	var err error = nil

	switch {
	case prod.Partitioner == RoundRobinPartitioner:
		chosenPartition, err = p.partitionInRoundRobin(message, numPartitions)
	case prod.Partitioner == HashPartitioner:
		chosenPartition, err = p.partitionByHash(message, numPartitions)
	case prod.Partitioner == RandomPartitioner:
		chosenPartition, err = p.partitionInRandom(message, numPartitions)
	case prod.Partitioner == ManualPartitioner:
		chosenPartition = message.Partition
	}

	if chosenPartition == -1 {
		return -1, ErrInvalidPartitioner
	}

	incrementPartitionCounter(prod, chosenPartition, numPartitions)
	// Increment the stats counter of total number of messages sent
	prod.NumMsgSentCounter.Incr()

	return chosenPartition, err
}

func (p *msgqPartitioner) RequiresConsistency() bool {
	return true
}

func checkIfExcludelistPartition(choosen int32, message *sarama.ProducerMessage) (int32, error) {
	prod := GetProducerRef(message)
	if prod == nil {
		mlog.Error("Message %v metadata does not contain producer reference. Failed to check if partition is in exclude list", message)
		return -1, ErrMalformedMessage
	}

	//Check if choosen partition is in blacklist
	for i := range prod.ExcludeListPartitions {
		if prod.ExcludeListPartitions[i] == int(choosen) {
			return -1, ErrExcludeListPartition
		}
	}

	return choosen, nil
}

func (p *msgqPartitioner) partitionByHash(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		mlog.Error("Null Key in the message %v for HashPartitioner", message)
		return -1, ErrNullKey
	}

	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}

	p.hasher.Reset()
	_, err = p.hasher.Write(bytes)
	if err != nil {
		return -1, err
	}

	hash := int32(p.hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}

	choosen, err := checkIfExcludelistPartition((hash % numPartitions), message)
	if err == ErrExcludeListPartition {
		mlog.Debug("The partition %d value to which the message key hashes is blacklisted. Cannot send the message", (hash % numPartitions))
	}

	return choosen, err
}

func (p *msgqPartitioner) partitionByIndex(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		mlog.Error("Null Key in the message %v for IndexPartitioner", message)
		return -1, ErrNullKey
	}

	ibytes, err := message.Key.Encode()
	if err != nil {
		mlog.Error("Failed to encode the message key %v", err)
		return -1, err
	}

	buf := bytes.NewReader(ibytes)
	var part uint8
	err = binary.Read(buf, binary.LittleEndian, &part)
	if err != nil {
		mlog.Error("Failed to read the message key. err %v", err)
		return -1, err
	}

	choosen, err := checkIfExcludelistPartition(int32(part), message)
	if err == ErrExcludeListPartition {
		mlog.Debug("The partition %d value to which the message key hashes is blacklisted. Cannot send the message", part)
	}

	return choosen, err
}

func (p *msgqPartitioner) partitionInRandom(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	//Choose a partition in random, but make sure its not one of the blacklisted partitions
	prod := GetProducerRef(message)
	if prod == nil {
		mlog.Error("Message %v metadata does not container producer object", message)
		return -1, ErrMalformedMessage
	}

	for {
		part := int32(p.generator.Intn(int(numPartitions)))
		//Check if choosen partition is in blacklist
		choosen, err := checkIfExcludelistPartition(part, message)
		if err == nil {
			return choosen, nil
		}
	}
}

func (p *msgqPartitioner) partitionInRoundRobin(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	//Choose a partition in round robin fashion, excluding the blacklisted partitions
	prod := GetProducerRef(message)
	if prod == nil {
		mlog.Error("Message %v metadata does not container producer object", message)
		return -1, ErrMalformedMessage
	}

	choosen := p.partition

	for {
		choosen++
		if choosen >= numPartitions {
			choosen = 0
		}

		//Check if choosen partition is in blacklist
		_, err := checkIfExcludelistPartition(choosen, message)
		if err == nil {
			p.partition = choosen
			return choosen, nil
		}

		if choosen == p.partition {
			mlog.Debug("The partition %d value to which the message key hashes is blacklisted. Cannot send the message", choosen)
			return -1, ErrExcludeListPartition
		}

	}
}
