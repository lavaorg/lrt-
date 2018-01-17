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

package stats

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/verizonlabs/northstar/pkg/mlog"
)

type (
	Stats struct {
		module    string
		interval  uint64
		ticker    *time.Ticker
		counters  []*Counter
		sets      []*Set
		timers    []*Timer
		timerPool sync.Pool
		lock      sync.RWMutex
	}

	Counter struct {
		name  string
		value int64
		lock  sync.RWMutex
	}

	Set struct {
		name  string
		value interface{}
		lock  sync.RWMutex
	}

	Timer struct {
		name  string
		start time.Time
		took  string
		stats *Stats
	}

	Event map[string]interface{}
)

// New starts a stats daemon and returns the instance.
func New(module string) (s *Stats) {
	// Read interval from env var
	interval, _ := strconv.ParseUint(os.Getenv("STATS_INTERVAL"), 10, 0)
	if interval == 0 {
		interval = 5
	}

	s = &Stats{
		module:   module,
		interval: interval,
	}
	s.timerPool.New = func() interface{} {
		return new(Timer)
	}
	s.ticker = time.NewTicker(time.Duration(s.interval) * time.Second)
	go s.start()
	return
}

// Disable disables logging stats
func (s *Stats) Disable() {
	s.ticker.Stop()
}

//---------
// Counter
//---------

// NewCounter returns a Counter instance.
func (s *Stats) NewCounter(name string) (c *Counter) {
	c = &Counter{name: name}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.counters = append(s.counters, c)
	return
}

// RemoveCounter will remove the counter from stats list
func (s *Stats) RemoveCounter(c *Counter) {
	// Dump the stats before removing
	e := s.aggregate()
	s.write(time.Now(), e)

	s.lock.Lock()
	defer s.lock.Unlock()
	//Remove the counter from list
	for i, obj := range s.counters {
		if obj == c {
			s.counters = append(s.counters[:i], s.counters[i+1:]...)
			break
		}
	}
}

// Incr increments the counter.
func (c *Counter) Incr() {
	// c.lock.Lock()
	// defer c.lock.Unlock()
	// c.value++
	atomic.AddInt64(&c.value, 1)
}

// IncrBy increments the counter by a value.
func (c *Counter) IncrBy(v int64) {
	atomic.AddInt64(&c.value, v)
}

// DecrBy decrements the counter by a value.
func (c *Counter) DecrBy(v int64) {
	atomic.AddInt64(&c.value, -v)
}

// Decr decrements the counter.
func (c *Counter) Decr() {
	// c.lock.Lock()
	// defer c.lock.Unlock()
	// c.value--
	atomic.AddInt64(&c.value, -1)
}

// Value returns the current value of the counter.
func (c *Counter) Value() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.value
}

//-----
// Set
//-----

// NewSet returns a Set instance.
func (s *Stats) NewSet(name string) (st *Set) {
	st = &Set{name: name}
	s.sets = append(s.sets, st)
	return
}

// Set sets a value.
func (s *Set) Set(value interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.value = value
}

// Get gets a value.
func (s *Set) Get() interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.value
}

//-------
// Timer
//-------

// NewTimer starts the timer.
func (s *Stats) NewTimer(name string) (t *Timer) {
	t = s.timerPool.Get().(*Timer)
	t.name = name
	t.stats = s
	t.start = time.Now()
	return
}

// Stop stops the timer.
func (t *Timer) Stop() {
	t.took = time.Now().Sub(t.start).String()
	t.stats.lock.Lock()
	t.stats.timers = append(t.stats.timers, t)
	t.stats.lock.Unlock()
}

// Interval sets interval (seconds) to write data to stdout / stderr.
// Default is 5 seconds.
func (s *Stats) Interval(n uint64) {
	s.interval = n
	s.ticker.Stop()
	s.ticker = time.NewTicker(time.Duration(s.interval) * time.Second)
	go s.start()

}

func (s *Stats) start() {
	for now := range s.ticker.C {
		e := s.aggregate()
		s.write(now, e)
	}
}

func (s *Stats) aggregate() (e Event) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	e = make(Event)
	e["module"] = s.module
	e["etime"] = time.Now().Unix()
	e["interval"] = s.interval

	// Counter
	for _, c := range s.counters {
		e[c.name] = c.value
		// Reset counter
		atomic.SwapInt64(&c.value, 0)
	}

	// Set
	for _, st := range s.sets {
		st.lock.RLock()
		defer st.lock.RUnlock()
		e[st.name] = st.value
	}

	// Timer
	for _, tm := range s.timers {
		e[tm.name] = tm.took
		// Put it back in the pool
		s.timerPool.Put(tm)
	}

	// Reset timer list
	s.timers = nil

	return
}

func (s *Stats) write(now time.Time, e Event) {
	b, err := json.Marshal(e)
	if err != nil {
		mlog.Emit(mlog.ERROR, err.Error())
	} else {
		mlog.Emit(mlog.STAT, string(b))
	}
}

// Dump returns statistics as JSON.
func (s *Stats) Dump() ([]byte, error) {
	event := s.aggregate()
	return json.Marshal(event)
}
