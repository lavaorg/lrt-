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
	"github.com/verizonlabs/northstar/pkg/config"
	"time"
)

var (
	RetryStartInterval, _  = config.GetInt("DKT_RETRY_START_INTERVAL", 1)
	RetryMaxBackoff, _     = config.GetInt("DKT_MAX_BACKOFF_INTERVAL_FOR_RETRY", 20)
	RetryMaxBeforeAlarm, _ = config.GetInt("DKT_MAX_RETRY_BEFORE_ALARM", 4)
	NumRetries, _          = config.GetInt("DKT_NUM_RETRIES", 100)
)

const (
	SubscribeChanBufferedItems int = 100
	DefaultZkConnectionTimeout = 6 * time.Second
)
