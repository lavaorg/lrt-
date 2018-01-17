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

package config

import (
	"time"

	"github.com/verizonlabs/northstar/pkg/config"
)

var (
	RetryStartInterval, _       = config.GetInt("DKT_RETRY_START_INTERVAL", 1)
	RetryMaxBackoff, _          = config.GetInt("DKT_MAX_BACKOFF_INTERVAL_FOR_RETRY", 20)
	RetryMaxBeforeAlarm, _      = config.GetInt("DKT_MAX_RETRY_BEFORE_ALARM", 4)
	NumRetries, _               = config.GetInt("DKT_NUM_RETRIES", 100)
	LatencySampleSize, _        = config.GetUInt64("DKT_LATENCY_SAMPLE_SIZE", 10000)
	AvailMonFeature, _          = config.GetBool("DKT_TOPIC_AVAIL_MON_FEATURE", false)
	MaxMsgFetchSize, _          = config.GetInt("DKT_MAX_MSG_FETCH_SIZE", MAX_MSG_FETCH_SIZE)
	OffsetUpdateInterval, _     = config.GetInt("OFFSET_UPDATE_INTERVAL_SEC", 5)
	WaitForKafkaAndZkServers, _ = config.GetBool("DKT_WAIT_FOR_KAFKA_AND_ZK_SERVERS", true)
)

const (
	MaxBufferTime      time.Duration = 100 * time.Millisecond
	MaxBufferedBytes   int           = 10485760 //10MB
	DialTimeout        time.Duration = 3 * time.Second
	NetReadTimeout     time.Duration = 3 * time.Second
	NetWriteTimeout    time.Duration = 3 * time.Second
	ChannelBufferSize  int           = 8192
	ProducerRetryMax   int           = 4
	MAX_MSG_FETCH_SIZE               = 32768
)
