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
	"github.com/Shopify/sarama"
	vzbreaker "github.com/Shopify/sarama/vz/breaker"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"sync"
	"time"
)

type PartitionStatus int
type AppEventT int // TODO: Check for type aliasing
type ConnStatus int

const (
	UP PartitionStatus = iota
	DOWN
	WARN
)
const (
	NOTIF_UP AppEventT = iota
	NOTIF_DOWN
	NOTIF_WARN
)

const (
	OK ConnStatus = iota
	BROKEN
	WARNING
)

type kafkaAvailMonitor struct {
	msgQRef    *MsgQ
	sync.Mutex // ← this mutex protects running flag
	running    bool
}

func (kafkaMon *kafkaAvailMonitor) testAndSetMonitorRunning() bool {
	kafkaMon.Lock()
	st := kafkaMon.running
	kafkaMon.running = true
	defer kafkaMon.Unlock()
	return st
}

func (kafkaMon *kafkaAvailMonitor) resetMonitorRunning() {
	kafkaMon.Lock()
	defer kafkaMon.Unlock()
	kafkaMon.running = false
}

func (kafkaMon *kafkaAvailMonitor) KafkaAvailAnalyzer() {
	for {
		brEvt := <-vzbreaker.GetBreakerChann()
		if brEvt == nil {
			// This condition should never happend...Just to be safe if channel get some error
			mlog.Error("Recieved empty error message")
		} else {
			// TODO: In future, It may require to down all the partition if partition is -1 but right now such condition found
			if connSt := kafkaMon.isSeriousConnError(brEvt.Err); (WARNING == connSt) || (BROKEN == connSt) {
				mlog.Info("Got some serious connection issue... Need to monitor kafka availibility status")
				if !kafkaMon.testAndSetMonitorRunning() {
					mlog.Info("Starting detector")
					go kafkaMon.detectKafkaAvailability()
				}

				// Check if already notified failure to application then no need to resend notification
				topicProducer := GetProducerRefByTopic(kafkaMon.msgQRef.Producers, brEvt.Topic)
				// Determine if only partition or all the paertition are affected
				list := topicProducer.GetAffectedPartList(brEvt.Partition)
				for iPart := range list {
					// Mark this partition down
					partSt := topicProducer.SetPartitionStatus(list[iPart], DOWN)
					mlog.Error("KafkaAvailAnalyzer: Send failed with error %v", brEvt.Err, "topic name: ", brEvt.Topic, "partition: ", list[iPart])
					kafkaMon.notifyTopicStatus(topicProducer.TopicName, NOTIF_DOWN, partSt)
				}
			}
		}
	}
}

func (kafkaMon *kafkaAvailMonitor) detectKafkaAvailability() {
	var kafkaClusterHealthy bool = false
	var isTopicHealthy bool = false
	var allTopicHealthy bool = true

	for !kafkaClusterHealthy {

		// Just wait a little bit...because thing take time to recover
		time.Sleep(3 * time.Second)
		mlog.Debug("waiting for 3 second before retry...")

		allTopicHealthy = true

		for _, topicProducer := range kafkaMon.msgQRef.Producers {

			isTopicHealthy = true

			// Refesh metadata first to avoid believing on stale information.
			err := kafkaMon.msgQRef.Client.RefreshMetadata(topicProducer.TopicName)

			if err != nil {
				mlog.Error("Metadata refresh error: ", err)
				// It's OK. This will happen only in case of whole cluster is down or unreachable
			}

			for partition, partitionStatus := range topicProducer.PartitionSt {

				if !topicProducer.CheckPartitionBlackListed(int32(partition)) {

					var brokerRef *sarama.Broker
					var err error
					var isPartitionHealthy bool = true

					mlog.Debug("Checking Topic: ", topicProducer.TopicName, "partition: ", partition, " and status array: ", partitionStatus)

					if brokerRef, err = kafkaMon.msgQRef.Client.Leader(topicProducer.TopicName, int32(partition)); nil != err {
						mlog.Debug("Leader Call failed with error: ", err)
						isPartitionHealthy = false
					} else if nil == brokerRef {
						mlog.Debug("Leader connection is not available: ", err)
						isPartitionHealthy = false
					} else {
						if connected, err := brokerRef.Connected(); !connected {
							mlog.Debug("broker is not connected: ", err)
							isPartitionHealthy = false
						} else {
							go func(prod *Producer, part int, brokerRef *sarama.Broker) {
								// Wait before sending status so that leftover error doesn't fluctuate the status unneccessarily
								//time.Sleep(1 * time.Second)
								mlog.Info("Leader call successful: ", prod.TopicName, part, prod.PartitionSt, "broker address is: ", brokerRef.Addr())
								if UP != prod.GetPartitionStatus(int32(part)) {
									mlog.Debug("broker is connected now.. sending notification to application : ")
									partSt := prod.SetPartitionStatus(int32(part), UP)
									kafkaMon.notifyTopicStatus(prod.TopicName, NOTIF_UP, partSt)
								} else {
									mlog.Debug("Leader call Successful but no need to send notif to application: ", prod.TopicName, part, prod.PartitionSt)
								}

							}(topicProducer, partition, brokerRef)
						}
					}
					if !isPartitionHealthy && DOWN != topicProducer.GetPartitionStatus(int32(partition)) {
						mlog.Info("detected partition: ", partition, " down")
						partSt := topicProducer.SetPartitionStatus(int32(partition), DOWN)
						kafkaMon.notifyTopicStatus(topicProducer.TopicName, NOTIF_DOWN, partSt)
					}
					// If any partition unhealthy then mark topic unhealthy
					if !isPartitionHealthy {
						isTopicHealthy = false
					}
				}
			}

			if false == isTopicHealthy {
				allTopicHealthy = false
			}
		}
		if allTopicHealthy {
			kafkaClusterHealthy = true
		}
	}

	// Set monitor started true
	kafkaMon.resetMonitorRunning()
	mlog.Info("All topics are healthy now ... availability monitor is exiting now...")
}

func (kafkaMon *kafkaAvailMonitor) notifyTopicStatus(topicName string, appEvt AppEventT, partSt []PartitionStatus) {

	evt := &TopicAvailEvent{
		TopicName:  topicName,
		EventType:  appEvt,
		PartStatus: partSt,
	}

	kafkaMon.msgQRef.TopicAvailChan <- evt
}

func (kafkaMon *kafkaAvailMonitor) isSeriousConnError(err error) ConnStatus {
	var connSt ConnStatus = OK
	switch {
	case err == sarama.ErrOutOfBrokers:
		connSt = BROKEN
	case err == sarama.ErrClosedClient:
		connSt = BROKEN
	case err == breaker.ErrBreakerOpen:
		connSt = BROKEN
	}

	return connSt
}
