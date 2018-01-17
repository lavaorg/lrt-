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
	"os"
	"testing"
	"time"
)

func TestCounter(t *testing.T) {
	s := New("test")
	c := s.NewCounter("requests")

	c.Incr()
	if c.value != 1 {
		t.Error("counter should be 1")
	}

	c.IncrBy(55)
	if c.value != 56 {
		t.Error("counter should be 56")
	}

	c.DecrBy(55)
	if c.value != 1 {
		t.Error("counter should be 1")
	}

	c.Decr()
	if c.value != 0 {
		t.Error("count should be 0")
	}

	c.IncrBy(10)
	if c.value != 10 {
		t.Error("counter should be 56")
	}
	s.RemoveCounter(c)
}

func TestSet(t *testing.T) {
	s := New("test")
	st := s.NewSet("users")

	st.Set(89)
	if st.Get() != 89 {
		t.Errorf("set value should be 89, found %v", st.value)
	}
}

func TestTimer(t *testing.T) {
	s := New("test")
	tm := s.NewTimer("encode")

	tm.Stop()
	if tm.took == "" {
		t.Error("timer should have a value")
	}
}

func BenchmarkCounter(b *testing.B) {
	s := New("test")
	c := s.NewCounter("requests")
	for n := 0; n < b.N; n++ {
		c.Incr()
	}
}

func BenchmarkCounterMultipleGoRoutines(b *testing.B) {
	s := New("test")
	c := s.NewCounter("requests")
	for n := 0; n < b.N; n++ {
		go c.Incr()
	}
}

func BenchmarkSet(b *testing.B) {
	s := New("test")
	st := s.NewSet("city")
	for n := 0; n < b.N; n++ {
		st.Set(n)
	}
}

func BenchmarkSetMultipleGoRoutines(b *testing.B) {
	s := New("test")
	st := s.NewSet("city")
	for n := 0; n < b.N; n++ {
		go st.Set(n)
	}
}

func BenchmarkTimer(b *testing.B) {
	s := New("test")
	for n := 0; n < b.N; n++ {
		tm := s.NewTimer("encode")
		tm.Stop()
	}
}

func BenchmarkTimerMultipleGoRoutines(b *testing.B) {
	s := New("test")
	for n := 0; n < b.N; n++ {
		go func() {
			tm := s.NewTimer("encode")
			tm.Stop()
		}()
	}
}

func TestStats(t *testing.T) {
	s := New("test")

	s.Interval(2)

	// Counter
	c1 := s.NewCounter("requests")
	c2 := s.NewCounter("responses")
	c1.Incr()
	c2.IncrBy(5)

	// Set
	st := s.NewSet("city")
	st.Set("London")

	// Timer
	tm := s.NewTimer("encode")
	tm.Stop()

	// Dump
	b, err := s.Dump()
	if err != nil {
		t.Error(err)
	} else {
		os.Stdout.Write(b)
	}

	time.Sleep(10 * time.Second)
}
