/* Copyright(C) 2018 Larry Rau. All rights reserved */

package utils

import stdbreaker "github.com/eapache/go-resiliency/breaker"
import "github.com/lavaorg/lrt/x/msgq/config"
import "time"
import "strings"
import "fmt"

type breakerSt uint32

const (
	closed breakerSt = iota
	open
)

type VzBreakerTopicPartStatus struct {
	Topic     string
	Partition int32
	Err       error
}

type Breaker struct {
	VzBreaker *stdbreaker.Breaker
	state     breakerSt
	topic     string
	partition int32
}

var VzBrTopPartSt chan *VzBreakerTopicPartStatus

func GetBreakerChann() chan *VzBreakerTopicPartStatus {
	return VzBrTopPartSt
}

func New(errorThreshold, successThreshold int, timeout time.Duration, topic string, partition int32) *Breaker {

	br := stdbreaker.New(errorThreshold, successThreshold, timeout)

	return &Breaker{
		VzBreaker: br,
		state:     closed,
		topic:     topic,
		partition: partition,
	}
}

func (b *Breaker) Run(work func() error) error {
	state := b.VzBreaker.Run(work)

	if stdbreaker.ErrBreakerOpen == state {
		// Check if previously status was closed and its open now
		// if yes then send notification to message
		if closed == b.state {
			// Send notification
			if true == config.AvailMonFeature {
				VzBrTopPartSt <- &VzBreakerTopicPartStatus{b.topic, b.partition, state}
			}
			// Change state
			b.state = open
		}
	} else {
		strErr := fmt.Sprintf("%s", state)
		if (open == b.state) && (strings.Contains(strErr, "client has run out of available brokers to talk to") || strings.Contains(strErr, "In the middle of a leadership election")) {
			// In the middle of a leadership election or err out of breaker...Don't change state
		} else {
			b.state = closed
		}
	}

	return state
}

type MessageMetadata struct {
	EnqueuedAt    time.Time
	TcpTxDuration time.Duration
	ProdRef       interface{}
}

const (
	INVALID_PARTITION int32 = -1
)

func (mm *MessageMetadata) Producer() interface{} {
	return mm.ProdRef
}

func (mm *MessageMetadata) GetMsgDelay() (time.Duration, time.Duration) {
	return time.Since(mm.EnqueuedAt), mm.TcpTxDuration
}

func (mm *MessageMetadata) SetBatchNwDelay(tcpTxDuration time.Duration) {
	mm.TcpTxDuration = tcpTxDuration
}
