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
	"errors"
	"os"
	"github.com/verizonlabs/northstar/pkg/config"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/redis"
	"github.com/verizonlabs/northstar/pkg/servdisc"
	"strings"
)

var (
	WebPort, _             = config.GetInt("RTE_PORT", 8080)
	WorkerQueueCapacity, _ = config.GetInt("RTE_WORKER_QUEUE_CAPACITY", 10)
	EnableRLimit, _        = config.GetBool("RTE_ENABLE_RLIMIT", true)
	GoMaxProcs, _          = config.GetInt("GOMAXPROCS", 1)
)

const (
	RTE_SERVICE_NAME = "rte"
)

func GetRTEId() (string, error) {
	id := os.Getenv("MESOS_TASK_ID")
	if id == "" {
		return "", errors.New("Please set MESOS_TASK_ID!")
	}

	split := strings.Split(id, ".")
	if len(split) < 2 {
		return "", errors.New("MESOS_TASK_ID has wrong format! Needs be: <name>.<id>")
	}

	realID := split[1]
	if realID == "" {
		return "", errors.New("ID part is empty")
	}

	return realID, nil
}

func GetNorthStarApiHostPort() (string, error) {
	hostPort := os.Getenv("NORTHSTARAPI_HOST_PORT")
	if hostPort == "" {
		return "", errors.New("Please set NORTHSTARAPI_HOST_PORT!")
	}

	return hostPort, nil
}

func CreateRedisCluster() (*redis.Redis, error) {
	hostPort, err := servdisc.GetHostPortStrings(servdisc.REDIS_SERVICE)
	if len(hostPort) == 0 || hostPort[0] == "" {
		mlog.Error("Failed to connect to Redis: %v", err)
		return nil, err
	}

	mlog.Info("Redis host port: %v", hostPort)
	return redis.NewRedisCluster(hostPort), nil
}
