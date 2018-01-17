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
	"crypto/rand"
	"encoding/base64"
	"os"
	"strconv"
	"sync"
	"time"
)

const luaRefresh = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`
const luaRelease = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
const minWaitRetry = 10 * time.Millisecond
var defLockTimeOut = getDuration("DEFAULT_REDIS_LOCK_TIMEOUT", 5 * time.Second)

type Lock struct {
	client Store
	key    string
	opts   LockOptions

	token string
	mutex sync.Mutex
}

type LockOptions struct {
	// The maximum duration to lock a key for
	// Default: 5s
	LockTimeout time.Duration

	// The maximum amount of time you are willing to wait to obtain that lock
	// Default: 0 = do not wait
	WaitTimeout time.Duration

	// In case WaitTimeout is activated, this it the amount of time you are willing
	// to wait between retries.
	// Default: 100ms, must be at least 10ms
	WaitRetry time.Duration
}

func (o *LockOptions) normalize() *LockOptions {
	if o == nil {
		o = new(LockOptions)
	}
	if o.LockTimeout < 1 {
		o.LockTimeout = defLockTimeOut
	}
	if o.WaitTimeout < 0 {
		o.WaitTimeout = 0
	}
	if o.WaitRetry < minWaitRetry {
		o.WaitRetry = minWaitRetry
	}
	return o
}

// ObtainLock is a shortcut for NewLock().Lock()
func ObtainLock(client Store, key string, opts *LockOptions) (*Lock, error) {
	lock := NewLock(client, key, opts)
	if ok, err := lock.Lock(); err != nil || !ok {
		return nil, err
	}
	return lock, nil
}

// NewLock creates a new distributed lock on key
func NewLock(client Store, key string, opts *LockOptions) *Lock {
	if opts == nil {
		opts = new(LockOptions)
	}
	return &Lock{client: client, key: key, opts: *opts.normalize()}
}

// IsLocked returns true if a lock is acquired
func (l *Lock) IsLocked() bool {
	l.mutex.Lock()
	locked := l.token != ""
	l.mutex.Unlock()

	return locked
}

// Lock applies the lock, don't forget to defer the Unlock() function to release the lock after usage
func (l *Lock) Lock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.token != "" {
		return l.refresh()
	}
	return l.create()
}

// Unlock releases the lock
func (l *Lock) Unlock() error {
	l.mutex.Lock()
	err := l.release()
	l.mutex.Unlock()

	return err
}

// Helpers

func (l *Lock) create() (bool, error) {
	l.reset()

	// Create a random token
	token, err := randomToken()
	if err != nil {
		return false, err
	}

	// Calculate the timestamp we are willing to wait for
	stop := time.Now().Add(l.opts.WaitTimeout)
	for {
		// Try to obtain a lock
		ok, err := l.obtain(token)
		if err != nil {
			return false, err
		} else if ok {
			l.token = token
			return true, nil
		}

		if time.Now().Add(l.opts.WaitRetry).After(stop) {
			break
		}
		time.Sleep(l.opts.WaitRetry)
	}
	return false, nil
}

func (l *Lock) refresh() (bool, error) {
	ttl := strconv.FormatInt(int64(l.opts.LockTimeout/time.Millisecond), 10)
	args := []string{l.token, ttl}
	status, err := l.client.Eval(luaRefresh, []string{l.key}, args)
	if err != nil {
		return false, err
	} else if status == int64(1) {
		return true, nil
	}
	return l.create()
}

func (l *Lock) obtain(token string) (bool, error) {
	ok, err := l.client.SetNX(l.key, token, l.opts.LockTimeout)
	if err == Nil {
		err = nil
	}
	return ok, err
}

func (l *Lock) release() error {
	defer l.reset()

	_, err := l.client.Eval(luaRelease, []string{l.key}, []string{l.token})
	if err == Nil {
		err = nil
	}
	return err
}

func (l *Lock) reset() {
	l.token = ""
}

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf), nil
}

func getDuration(envStr string, defVal time.Duration) time.Duration {
	val := os.Getenv(envStr)
	if val == "" {
		return defVal
	}
	if s, err := strconv.ParseInt(val, 10, 64); err == nil {
		return time.Duration(s)
	} else {
		return defVal
	}
}
