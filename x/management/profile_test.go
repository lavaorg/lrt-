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

package management

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestProfile(t *testing.T) {
	convey.Convey("Test Basic Profile Service", t, func() {
		profile := NewProfile("my_profile")
		convey.So(profile, convey.ShouldNotBeNil)

		convey.Convey("Test Create Counter", func() {
			counter := profile.AddCounter("my_counter", 1)
			convey.So(counter, convey.ShouldNotBeNil)
			convey.So(counter.Count(), convey.ShouldEqual, 1)
		})
		convey.Convey("Test Create Timer", func() {
			// the timer tests below depend on the size = 3
			timer := profile.AddTimer("my_timer", 3)
			convey.So(timer, convey.ShouldNotBeNil)
			convey.So(timer.AvgDuration(), convey.ShouldEqual, time.Duration(0))
		})
		convey.Convey("Test Create Rate", func() {
			// the rate tests below depend on the size = 3
			rate := profile.AddRate("my_rate", 3)
			convey.So(rate, convey.ShouldNotBeNil)
			convey.So(rate.AvgRate(), convey.ShouldEqual, time.Duration(0))
		})
	})
}

func TestCounter(t *testing.T) {
	convey.Convey("Test Counter Reporter", t, func() {
		// get previously created counter
		counter, found := NewProfile("my_profile").Counter("my_counter")
		convey.So(found, convey.ShouldBeTrue)
		convey.So(counter, convey.ShouldNotBeNil)

		convey.Convey("Test Counter Inc and Dec", func() {
			counter.Inc()
			time.Sleep(1 * time.Millisecond)
			convey.So(counter.Count(), convey.ShouldEqual, 2)

			counter.Dec()
			time.Sleep(1 * time.Millisecond)
			convey.So(counter.Count(), convey.ShouldEqual, 1)
		})
		convey.Convey("Test Counter Rollover Prevention", func() {
			counter.SetCount(MaxInt)
			time.Sleep(1 * time.Millisecond)
			convey.So(counter.Count(), convey.ShouldEqual, MaxInt)

			counter.Inc()
			time.Sleep(1 * time.Millisecond)
			convey.So(counter.Count(), convey.ShouldEqual, MaxInt)

			counter.SetCount(MinInt)
			time.Sleep(1 * time.Millisecond)
			convey.So(counter.Count(), convey.ShouldEqual, MinInt)

			counter.Dec()
			time.Sleep(1 * time.Millisecond)
			convey.So(counter.Count(), convey.ShouldEqual, MinInt)
		})
	})
}

func TestTimer(t *testing.T) {
	convey.Convey("Test Timer Reporter", t, func() {
		// get and check previous created timer
		timer, found := NewProfile("my_profile").Timer("my_timer")
		convey.So(found, convey.ShouldBeTrue)
		convey.So(timer, convey.ShouldNotBeNil)

		convey.Convey("Test Sparse Avg Duration", func() {
			// create a single 2 sec duration
			timer.AddDuration(2 * time.Second)

			// allow context switch before read
			time.Sleep(1 * time.Millisecond)

			// avg duration should be 2 sec
			convey.So(timer.AvgDuration(), convey.ShouldEqual, 2*time.Second)
		})
		convey.Convey("Test Add Duration Beyond Size", func() {
			// create a full set of durations (plus one from above)
			for i := 0; i < timer.Size(); i++ {
				timer.AddDuration(1 * time.Second)
			}

			// allow context switch before read
			time.Sleep(1 * time.Millisecond)

			// avg duration should be 1 sec
			convey.So(timer.AvgDuration(), convey.ShouldEqual, 1*time.Second)
		})
		convey.Convey("Test Correct Avg as Duration Changes", func() {
			// create new duration, assuming size = 3,
			// the avg should become ( 1 * 2 + 2 * 1 ) sec / 3 = 1.33 sec
			timer.AddDuration(2 * time.Second)

			// allow context switch before read
			time.Sleep(1 * time.Millisecond)

			// avg duration should be 1.666 sec
			convey.So(timer.AvgDuration(), convey.ShouldAlmostEqual, 1333*time.Millisecond, 1*time.Millisecond)

			// create new duration, assuming size = 3,
			// the avg should become ( 1 * 1 + 2 * 2 ) sec / 3 = 1.66 sec
			timer.AddDuration(2 * time.Second)

			// allow context switch before read
			time.Sleep(1 * time.Millisecond)

			// avg duration should be 1.3333 sec
			convey.So(timer.AvgDuration(), convey.ShouldAlmostEqual, 1666*time.Millisecond, 1*time.Millisecond)
		})
	})
}

func TestRate(t *testing.T) {
	convey.Convey("Test Rate Reporter", t, func() {
		// get and check previously created rate
		rate, found := NewProfile("my_profile").Rate("my_rate")
		convey.So(found, convey.ShouldBeTrue)
		convey.So(rate, convey.ShouldNotBeNil)

		convey.Convey("Test Sparse Avg Rate", func() {
			// create two samples, 1 sec apart
			rate.AddSample(time.Now())
			time.Sleep(2 * time.Second)
			rate.AddSample(time.Now())

			// allow context switch before read
			time.Sleep(1 * time.Millisecond)

			// avg rate should be 2 sec per sample
			convey.So(rate.AvgRate(), convey.ShouldAlmostEqual, 2*time.Second, 10*time.Millisecond)
		})
		convey.Convey("Test Add Sample Beyond Size", func() {
			// create full set of samples (plus the one above)
			for i := 0; i < rate.Size(); i++ {
				time.Sleep(1 * time.Second)
				rate.AddSample(time.Now())
			}

			// allow context switch before read
			time.Sleep(1 * time.Millisecond)

			// avg rate should now be 1 sec per sample
			convey.So(rate.AvgRate(), convey.ShouldAlmostEqual, 1*time.Second, 10*time.Millisecond)
		})
		convey.Convey("Test Correct Avg as Sample Rate Changes", func() {
			// assuming that the size of 3
			// then the added sample make the avg become, (1 * 2 + 2 * 1) sec / 3 = 1.33 sec
			time.Sleep(2 * time.Second)
			rate.AddSample(time.Now())

			// allow context switch
			time.Sleep(1 * time.Millisecond)

			// check avg rate
			convey.So(rate.AvgRate(), convey.ShouldAlmostEqual, 1333*time.Millisecond, 10*time.Millisecond)

			// again, assuming that the size = 3 (i.e., 4 samples will fill)
			// then the added sample make the avg become, (1 * 1 + 2 * 2) sec / 3 = 1.66 sec
			time.Sleep(2 * time.Second)
			rate.AddSample(time.Now())

			// allow context switch
			time.Sleep(1 * time.Millisecond)

			// check avg rate
			convey.So(rate.AvgRate(), convey.ShouldAlmostEqual, 1666*time.Millisecond, 10*time.Millisecond)
		})
		convey.Convey("Test Multithreaded Writes and Reads", func() {
			// start a bunch of read and write goroutines
			writeSync := make(chan bool, 0)
			readSync := make(chan bool, 0)
			for i := 0; i < 50; i++ {
				go rateWrites(t, rate, writeSync)
				go rateReads(t, rate, readSync)
			}
			<-writeSync
			<-readSync

			// allow final context switch
			time.Sleep(1 * time.Millisecond)

			// check avg rate (it should be very small)
			convey.So(rate.AvgRate(), convey.ShouldBeLessThan, 1*time.Millisecond)
		})
	})
}

func rateWrites(t *testing.T, rate *Rate, sync chan bool) {
	// add a sample
	rate.AddSample(time.Now())

	// sync
	sync <- true
}

func rateReads(t *testing.T, rate *Rate, sync chan bool) {
	// read avg rate
	rate.AvgRate()

	// sync
	sync <- true
}
