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

import (
	"time"

	redis "gopkg.in/redis.v5"
)

var Nil = redis.Nil

type (
	Redis struct {
		client        *redis.Client
		clusterClient *redis.ClusterClient
	}
)

// Init creates redis connection pool and returns a redis struct
func NewRedis(ad string) *Redis {
	r := &Redis{
		client: redis.NewClient(&redis.Options{
			Addr:     ad,
			Password: "",
			DB:       0,
		}),
	}
	return r
}

func NewRedisCluster(addresses []string) *Redis {
	r := &Redis{
		clusterClient: redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    addresses,
			Password: "",
		}),
	}
	return r
}

// If key already exists and is a string, this command appends the value at the end of the string.
func (r *Redis) Append(key, value string) (numStr int64, err error) {
	if r.client != nil {
		return r.client.Append(key, value).Result()
	}
	return r.clusterClient.Append(key, value).Result()
}

// Exists check if key exists
func (r *Redis) Exists(key string) (b bool, err error) {
	if r.client != nil {
		return r.client.Exists(key).Result()
	}
	return r.clusterClient.Exists(key).Result()
}

// HGet the value for field in hash stored at key
func (r *Redis) HExists(key string, field string) (b bool, err error) {
	if r.client != nil {
		return r.client.HExists(key, field).Result()
	}
	return r.clusterClient.HExists(key, field).Result()
}

// HGet the value for field in hash stored at key
func (r *Redis) HGet(key string, field string) (val string, err error) {
	if r.client != nil {
		return r.client.HGet(key, field).Result()
	}
	return r.clusterClient.HGet(key, field).Result()
}

// HGetAll all values for hash stored at key
func (r *Redis) HGetAll(key string) (ret map[string]string, err error) {
	if r.client != nil {
		return r.client.HGetAll(key).Result()
	}
	return r.clusterClient.HGetAll(key).Result()
}

// HIncrBy increments the counter for hash stored at the key.
func (r *Redis) HIncrBy(key, field string, val int64) (n int64, err error) {
	if r.client != nil {
		return r.client.HIncrBy(key, field, val).Result()
	}
	return r.clusterClient.HIncrBy(key, field, val).Result()
}

// Set sets key value pair.
func (r *Redis) Set(key string, val interface{}) error {
	if r.client != nil {
		return r.client.Set(key, val, 0).Err()
	}
	return r.clusterClient.Set(key, val, 0).Err()
}

// Set key to hold the string value and set key to timeout after a given number of seconds.
func (r *Redis) SetEX(key string, val interface{}, ttl int) error {
	if r.client != nil {
		return r.client.Set(key, val, time.Duration(ttl)*time.Second).Err()
	}
	return r.clusterClient.Set(key, val, time.Duration(ttl)*time.Second).Err()
}

// Set key to hold the string value and set key to timeout after a given number of seconds.
func (r *Redis) SetRange(key string, offset int64, val string) (int64, error) {
	if r.client != nil {
		return r.client.SetRange(key, offset, val).Result()
	}
	return r.clusterClient.SetRange(key, offset, val).Result()
}

// Get get a value for a given key.
func (r *Redis) Get(key string) (val string, err error) {
	if r.client != nil {
		return r.client.Get(key).Result()
	}
	return r.clusterClient.Get(key).Result()
}

// Returns the substring of the string value stored at key.
func (r *Redis) GetRange(key string, start, end int64) (val string, err error) {
	if r.client != nil {
		return r.client.GetRange(key, start, end).Result()
	}
	return r.clusterClient.GetRange(key, start, end).Result()
}

// GETSET can be used together with INCR for counting with atomic reset.
func (r *Redis) GetSet(key string, value interface{}) (val string, err error) {
	if r.client != nil {
		return r.client.GetSet(key, value).Result()
	}
	return r.clusterClient.GetSet(key, value).Result()
}

// HSet sets value at the hash stored at key
func (r *Redis) HSet(key string, field string, val string) (isnew bool, err error) {
	if r.client != nil {
		return r.client.HSet(key, field, val).Result()
	}
	return r.clusterClient.HSet(key, field, val).Result()
}

// Del deletes a key
func (r *Redis) Del(keys ...string) (keysDeleted int64, err error) {
	if r.client != nil {
		return r.client.Del(keys...).Result()
	}
	return r.clusterClient.Del(keys...).Result()
}

// HDel deletes key-value at hash stored at key
func (r *Redis) HDel(key string, fields ...string) (keysDeleted int64, err error) {
	if r.client != nil {
		return r.client.HDel(key, fields...).Result()
	}
	return r.clusterClient.HDel(key, fields...).Result()
}

// HLen Returns length of a hash stored at key
func (r *Redis) HLen(key string) (hashLen int64, err error) {
	if r.client != nil {
		return r.client.HLen(key).Result()
	}
	return r.clusterClient.HLen(key).Result()
}

// Expire Sets Expiry for the key to ttl seconds
func (r *Redis) Expire(key string, ttl int) (isnew bool, err error) {
	if r.client != nil {
		return r.client.Expire(key, time.Duration(ttl)*time.Second).Result()
	}
	return r.clusterClient.Expire(key, time.Duration(ttl)*time.Second).Result()
}

// SetNXWithTTL sets key value pair with TTL in seconds.
func (r *Redis) SetNXWithTTL(key string, val string, ttlInSeconds int) (isSet bool, err error) {
	if r.client != nil {
		return r.client.SetNX(key, val, time.Duration(ttlInSeconds)*time.Second).Result()
	}
	return r.clusterClient.SetNX(key, val, time.Duration(ttlInSeconds)*time.Second).Result()
}

// SetNX sets key value pair with TTL value nano seconds.
func (r *Redis) SetNX(key string, val string, ttl time.Duration) (isSet bool, err error) {
	if r.client != nil {
		return r.client.SetNX(key, val, ttl).Result()
	}
	return r.clusterClient.SetNX(key, val, ttl).Result()
}

// Lpush pushes into the list from left.
func (r *Redis) Lpush(list string, val ...interface{}) (elementsAdded int64, err error) {
	if r.client != nil {
		return r.client.LPush(list, val...).Result()
	}
	return r.clusterClient.LPush(list, val...).Result()
}

// Rpop pops from the list from right.
func (r *Redis) Rpop(list string) (val string, err error) {
	if r.client != nil {
		return r.client.RPop(list).Result()
	}
	return r.clusterClient.RPop(list).Result()
}

// LTrim trims the list from left.
func (r *Redis) LTrim(list string, start, stop int64) (string, error) {
	if r.client != nil {
		return r.client.LTrim(list, start, stop).Result()
	}
	return r.clusterClient.LTrim(list, start, stop).Result()
}

func (r *Redis) LIndex(list string, index int64) (string, error) {
	if r.client != nil {
		return r.client.LIndex(list, index).Result()
	}
	return r.clusterClient.LIndex(list, index).Result()
}

// Incr increments the counter.
func (r *Redis) Incr(key string) (n int64, err error) {
	if r.client != nil {
		return r.client.Incr(key).Result()
	}
	return r.clusterClient.Incr(key).Result()
}

func (r *Redis) Decr(key string) (n int64, err error) {
	if r.client != nil {
		return r.client.Decr(key).Result()
	}
	return r.clusterClient.Decr(key).Result()
}

func (r *Redis) Eval(script string, keys []string, args []string) (a interface{}, err error) {
	if r.client != nil {
		return r.client.Eval(script, keys, args).Result()
	}
	return r.clusterClient.Eval(script, keys, args).Result()
}

func (r *Redis) EvalSha(sha1 string, keys []string, args []string) (a interface{}, err error) {
	if r.client != nil {
		return r.client.EvalSha(sha1, keys, args).Result()
	}
	return r.clusterClient.EvalSha(sha1, keys, args).Result()
}

func (r *Redis) ScriptLoad(script string) (sha1 string, err error) {
	if r.client != nil {
		return r.client.ScriptLoad(script).Result()
	}
	return r.clusterClient.ScriptLoad(script).Result()
}
