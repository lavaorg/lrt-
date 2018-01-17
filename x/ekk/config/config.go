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

import "github.com/verizonlabs/northstar/pkg/config"

// Logging Topics
const (
	TopicStdout = "stdoutlogging"
	TopicStderr = "stderrlogging"
)

// Configuration
var (
	ServiceName, _           = config.GetString("DKT_EKK_SERVICE_NAME", "EKK")
	ElasticsearchURLs, _     = config.GetString("ELASTICSEARCH_URLS", "http://localhost:9200")
	NumWorkers, _            = config.GetInt("DKT_EKK_NUM_WORKERS", 64)
	WorkQueueSize, _         = config.GetInt("DKT_EKK_WORKER_QUEUE_SIZE", 1024)
	ElasticsearchTTL, _      = config.GetInt("ELASTICSEARCH_TTL", 7)
	ElasticsearchShards, _   = config.GetInt("ELASTICSEARCH_SHARDS", 5) // Generally equal to number of nodes or greater.
	ElasticsearchReplicas, _ = config.GetInt("ELASTICSEARCH_REPLICS", 1)
)
