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

package lualib

import (
	"fmt"
	"github.com/yuin/gopher-lua"
	"github.com/verizonlabs/northstar/pkg/redis"
)

var errorRedisNotConfigured = "REDIS Client not configured"

type Options struct {
	KeyPrefix string
}

type redisModule struct {
	options *Options
	client  redis.Store
}

func NewRedisModule(rClient redis.Store) *redisModule {
	return &redisModule{
		options: &Options{},
		client:  rClient,
	}
}

func NewRedisModuleWithOptions(rClient redis.Store, options *Options) *redisModule {
	if options == nil {
		options = &Options{}
	}

	return &redisModule{
		options: options,
		client:  rClient,
	}
}

func (r *redisModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"get":      r.get,
		"set":      r.set,
		"setnx":    r.setnx,
		"incr":     r.incr,
		"decr":     r.decr,
		"exists":   r.exists,
		"expire":   r.expire,
		"append":   r.append,
		"getrange": r.getrange,
		"setrange": r.setrange,
		"hget":     r.hget,
		"hset":     r.hset,
		"hgetall":  r.hgetall,
		"del":      r.del,
	})
	mod.RawSetString("Nil", lua.LString(redis.Nil))
	L.Push(mod)
	return 1
}

func (r *redisModule) set(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LString(errorRedisNotConfigured))
		return 1
	}
	k := L.CheckString(1)
	v := L.CheckAny(2).String()
	t := 0
	if L.GetTop() == 3 {
		t = L.CheckInt(3)
	}

	err := r.client.SetEX(getKeyWithPrefix(r.options.KeyPrefix, k), v, t)
	if err != nil {
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LNil)
	}
	return 1
}

func (r *redisModule) get(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	val, err := r.client.Get(getKeyWithPrefix(r.options.KeyPrefix, k))
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
		return 2
	}

	L.Push(lua.LString(val))
	L.Push(lua.LNil)
	return 2
}

func (r *redisModule) setnx(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	v := L.CheckString(2)
	t := L.CheckInt(3)
	b, err := r.client.SetNXWithTTL(getKeyWithPrefix(r.options.KeyPrefix, k), v, t)
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LBool(b))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) incr(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	val, err := r.client.Incr(getKeyWithPrefix(r.options.KeyPrefix, k))
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LNumber(val))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) decr(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	val, err := r.client.Decr(getKeyWithPrefix(r.options.KeyPrefix, k))
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LNumber(val))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) exists(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	b, err := r.client.Exists(getKeyWithPrefix(r.options.KeyPrefix, k))
	if err != nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LBool(b))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) expire(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	t := L.CheckInt(2)
	b, err := r.client.Expire(getKeyWithPrefix(r.options.KeyPrefix, k), t)
	if err != nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LBool(b))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) append(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	v := L.CheckString(2)
	val, err := r.client.Append(getKeyWithPrefix(r.options.KeyPrefix, k), v)
	if err != nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LNumber(val))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) getrange(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	s := L.CheckInt64(2)
	e := L.CheckInt64(3)
	val, err := r.client.GetRange(getKeyWithPrefix(r.options.KeyPrefix, k), s, e)
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LString(val))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) setrange(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	o := L.CheckInt64(2)
	v := L.CheckString(3)
	val, err := r.client.SetRange(getKeyWithPrefix(r.options.KeyPrefix, k), o, v)
	if err != nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LNumber(val))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) hset(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	f := L.CheckString(2)
	v := L.CheckString(3)
	b, err := r.client.HSet(getKeyWithPrefix(r.options.KeyPrefix, k), f, v)
	if err != nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LBool(b))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) hget(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	f := L.CheckString(2)
	val, err := r.client.HGet(getKeyWithPrefix(r.options.KeyPrefix, k), f)
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		L.Push(lua.LString(val))
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) hgetall(L *lua.LState) int {
	if r.client == nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	k := L.CheckString(1)
	val, err := r.client.HGetAll(getKeyWithPrefix(r.options.KeyPrefix, k))
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
	} else {
		tbl := L.NewTable()
		for ky, vl := range val {
			tbl.RawSetString(ky, lua.LString(vl))
		}
		L.Push(tbl)
		L.Push(lua.LNil)
	}
	return 2
}

func (r *redisModule) del(L *lua.LState) int {
	k := []string{}
	if r.client == nil {
		L.Push(lua.LNumber(0))
		L.Push(lua.LString(errorRedisNotConfigured))
		return 2
	}
	len := L.GetTop()
	for i := 1; i <= len; i++ {
		key := getKeyWithPrefix(r.options.KeyPrefix, L.CheckString(i))
		k = append([]string{key}, k...)
	}
	val, err := r.client.Del(k...)
	// In case requested keys not present there is no error, only 0 is returned
	if err != nil {
		L.Push(lua.LNumber(val))
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
		return 2
	}
	L.Push(lua.LNumber(val))
	return 1
}

func getKeyWithPrefix(prefix, key string) string {
	if prefix == "" || key == "" {
		return key
	}

	return fmt.Sprintf("%s_%s", prefix, key)
}
