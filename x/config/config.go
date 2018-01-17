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
	"github.com/verizonlabs/northstar/pkg/mlog"
	"strconv"
)

// default value is return if key does not exists

func GetString(key, def string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		mlog.Info("Env %s is not set, returning default value %s", key, def)
		return def, errors.New("Config value is empty, returning default value")
	} else {
		return val, nil
	}
}

func getString(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", errors.New("Config value is empty")
	} else {
		return val, nil
	}
}

// default value is return if env is not set

func GetBool(key string, def bool) (bool, error) {
	val, err := getString(key)
	if err != nil {
		mlog.Info("Env %s is not set, default value %t is used", key, def)
		return def, err
	}
	return strconv.ParseBool(val)
}

// default value is return if env is not set

func GetInt(key string, def int) (int, error) {
	val, err := getString(key)
	if err != nil {
		mlog.Info("Env %s is not set, default value %d is used", key, def)
		return def, err
	}
	return strconv.Atoi(val)
}

// default value is return if env is not set

func GetUInt64(key string, def uint64) (uint64, error) {
	val, err := getString(key)
	if err != nil {
		mlog.Info("Env %s is not set, default value %d will be used", key, def)
		return def, err
	}
	v, err := strconv.ParseInt(val, 10, 64)
	if v < 0 || err != nil {
		mlog.Info("Invalid value %d is set for Env %s, default value %d is used", v, key, def)
		return def, err
	}
	return uint64(v), nil
}

func SetString(key, val string) {
	os.Setenv(key, val)
}
