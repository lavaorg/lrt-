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

package redis

import "time"

// Store defines a key-value interface
type (
	Store interface {
		Append(key, value string) (int64, error)
		Decr(key string) (int64, error)
		Del(key ...string) (int64, error)
		Eval(script string, keys []string, args []string) (interface{}, error)
		EvalSha(sha1 string, keys []string, args []string) (interface{}, error)
		Exists(key string) (bool, error)
		Expire(key string, ttl int) (bool, error)
		Get(key string) (string, error)
		GetRange(key string, start, end int64) (string, error)
		GetSet(key string, value interface{}) (string, error)
		HDel(key string, field ...string) (int64, error)
		HExists(key, field string) (bool, error)
		HGet(key string, field string) (string, error)
		HGetAll(key string) (ret map[string]string, err error)
		HIncrBy(key string, field string, incr int64) (int64, error)
		HLen(key string) (int64, error)
		HSet(key string, field string, val string) (bool, error)
		Incr(key string) (int64, error)
		LIndex(list string, index int64) (string, error)
		LTrim(list string, start, stop int64) (string, error)
		Lpush(list string, val ...interface{}) (int64, error)
		Rpop(list string) (string, error)
		ScriptLoad(script string) (sha1 string, err error)
		Set(key string, val interface{}) error
		SetEX(key string, val interface{}, ttl int) error
		SetNXWithTTL(key string, val string, ttl int) (bool, error)
		SetNX(key string, val string, ttl time.Duration) (bool, error)
		SetRange(key string, offset int64, value string) (int64, error)
	}
)
