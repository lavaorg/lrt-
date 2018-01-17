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
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	profileLock sync.RWMutex
	profiles    map[string]*Profile = make(map[string]*Profile)
)

// Reporter
// A reporter writes profile information to the given io.Writer
// (e.g., for plain-text web-pages or logging) or JSON marshaller
type Reporter interface {
	WriteTo(w io.Writer)
	MarshalJSON() ([]byte, error)
}

// Profile
// A profile is a collection of reporters for a particular package
// or component.
type Profile struct {
	name      string
	reporters map[string]Reporter
}

// NewProfile
// Create a new package or component profile, or return a previously
// created profile by name. The name is usually in the form of dot
// delimited package name (not slash delimited).
func NewProfile(name string) *Profile {

	profileLock.RLock()
	profile, found := profiles[name]
	profileLock.RUnlock()

	if found {
		return profile
	} else {
		profile := new(Profile)
		profile.name = name
		profile.reporters = make(map[string]Reporter)

		profileLock.Lock()
		defer profileLock.Unlock()

		profiles[name] = profile
		return profile
	}
}

// Counter
// Get the named counter
func (this *Profile) Counter(name string) (*Counter, bool) {
	reporter, found := this.Reporter(name)
	if !found {
		return nil, false
	}
	counter, castable := reporter.(*Counter)
	if !castable {
		return nil, false
	}
	return counter, true
}

// Timer
// Get the named timer
func (this *Profile) Timer(name string) (*Timer, bool) {
	reporter, found := this.Reporter(name)
	if !found {
		return nil, false
	}
	timer, castable := reporter.(*Timer)
	if !castable {
		return nil, false
	}
	return timer, true
}

// Rate
// Get the named rate reporter
func (this *Profile) Rate(name string) (*Rate, bool) {
	reporter, found := this.Reporter(name)
	if !found {
		return nil, false
	}
	rate, castable := reporter.(*Rate)
	if !castable {
		return nil, false
	}
	return rate, true
}

// Reporter
// Get the named reporter
func (this *Profile) Reporter(name string) (interface{}, bool) {
	reporter, found := this.reporters[name]
	return reporter, found
}

// Report
func (this *Profile) WriteTo(w io.Writer) {
	fmt.Fprintf(w, "------------------------------------\n")
	fmt.Fprintf(w, "Profile Report: %v\n", this.name)
	fmt.Fprintf(w, "------------------------------------\n")
	for _, v := range this.reporters {
		v.WriteTo(w)
	}
}

// Custom JSON Marshaller
func (this *Profile) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":      this.name,
		"reporters": this.reporters,
	})
}

// //////////////////////////////////////////////////////////////////////////
// Custom Reporter
// A custom reporter implements the Reporter interface, and is used
// to add a report to the profile.
//
// Example:
// Lets say we wish to provide a current state of some connection, we'd add
// a custom reporter during my_package init(),
//
//    myStateMachine := new(StateMachine) // this implements management.Reporter
//    profile := management.Profiler("my_path.my_package")
//    profile.AddReporter("my state machine", myStateMachine)
//
// The state machine would then implement management.Reporter like so,
//
//    func (myStateMachine *StateMachine) WriteTo(w io.Writer) {
//      fmt.Fprintf(w, "current state, %v", myStateMachine.state.String())
//    }
//
//    func (myStateMachine *StateMachine) MarshalJSON() ([]byte,error) {
//      return json.Marshal(map[string]interface{}{
//        "type": "state",
//        "state": myStateMachine.state.String(),
//      })
//    }

// Add Customer Reporter to the Profile
// See the default profile type definitions (e.g., Counter, Timer or Rate)
// for examples of a Reporter
func (this *Profile) AddReporter(name string, reporter Reporter) {
	if reporter != nil {
		this.reporters[name] = reporter
	}
}

// //////////////////////////////////////////////////////////////////////////
// Default Counter Reporter
// A counter reporter is used to provide either a single int value to the
// parent profile, and is usually used to count occurances of an event.
//
// Example:
// Lets say we wish to count bad packets received, we could add a counter
// to the package profile during init(),
//
//    profile := management.Profiler("my_path.my_package")
//    badPacketCounter := profile.AddCounter( "bad_rcvs", 0 )
//
// Then, when a bad packet was found, we'd update the counter,
//
//    badPacketCounter.Inc()

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1) // on OSX,  9,223,372,036,854,775,807
const MinInt = -MaxInt - 1       // on OSX, -9,223,372,036,854,775,808

type Counter struct {
	name, template string
	lock           sync.RWMutex
	count          int
}

// AddCounter
// Add a Counter Reporter to the Profile
func (this *Profile) AddCounter(name string, initial int) *Counter {
	return this.AddCounterWithTemplate(name, "", initial)
}

// AddCounterWithTemplate
// Add a Counter Reporter to the Profile with a custom reporting string
func (this *Profile) AddCounterWithTemplate(name, template string, initial int) *Counter {
	counter := new(Counter)
	counter.name = name
	counter.template = template
	counter.count = initial
	this.AddReporter(name, counter)
	return counter
}

// Inc
// Increment the counter
func (this *Counter) Inc() {
	// avoid blocking the caller
	// Note this will require a context switch before the
	// count function returns the new value
	go func() {
		// Note that a mutex is used over a channel to avoid
		// a continuously running goroutine for each counter.
		// We're also avoided the atomic inc/dec for now.
		this.lock.Lock()
		defer this.lock.Unlock()

		// do not allow rollover
		if this.count < MaxInt {
			this.count += 1
		}
	}()
}

// Dec
// Decrement the counter
func (this *Counter) Dec() {
	// avoid blocking the caller
	// Note this will require a context switch before the
	// count function returns the new value
	go func() {
		// Note that a mutex is used over a channel to avoid
		// a continuously running goroutine for each counter.
		// We're also avoided the atomic inc/dec for now.
		this.lock.Lock()
		defer this.lock.Unlock()

		// do not allow rollover
		if this.count > MinInt {
			this.count -= 1
		}
	}()
}

// Count
// Return the current count value
//
// Note that this read is not syncronized by design, do not add Rlock
func (this *Counter) Count() int {
	return this.count
}

// SetCount
// Force the count to a particular value
func (this *Counter) SetCount(value int) {
	// avoid blocking the caller
	// Note this will require a context switch before the
	// count function returns the new value
	go func() {
		// Note that a mutex is used over a channel to avoid
		// a continuously running goroutine for each counter.
		this.lock.Lock()
		defer this.lock.Unlock()
		this.count = value
	}()
}

func (this *Counter) WriteTo(w io.Writer) {
	if this.template == "" {
		fmt.Fprintf(w, "\tcounter, %v = %v\n", this.name, this.Count())
	} else {
		fmt.Fprintf(w, this.template, this.Count())
	}
}

func (this *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":  "counter",
		"count": this.count,
	})
}

// //////////////////////////////////////////////////////////////////////////
// Default Timer Reporter
// A Timer Reporter is used to record the average duration between two points.
//
// Example:
// If the user wanted to measure the average time it takes to perform a
// task, they could do something like the following during package init()...
//
//    profile := management.NewProfile("my_path.my_package")
//    myTimer := profile.AddTimer("my_timer", 10) // take the average of 10 samples
//
// Then, when the call is being measured,
//
//    start = time.Now()
//    LongCall()
//    myTimer.AddDuration(time.Since(start))

type Timer struct {
	name, template string
	lock           sync.RWMutex
	duration       []time.Duration
	sparse         bool
	current        int
}

// AddTimer
// Add a Timer Reporter to the Profile
func (this *Profile) AddTimer(name string, size int) *Timer {
	return this.AddTimerWithTemplate(name, "", size)
}

// AddTimerWithTemplate
// Add a Timer Reporter with a custom reporting template string
func (this *Profile) AddTimerWithTemplate(name, template string, size int) *Timer {
	if size == 0 {
		return nil
	}
	timer := new(Timer)
	timer.name = name
	timer.template = template
	timer.duration = make([]time.Duration, size)
	timer.current = 0
	timer.sparse = true
	this.AddReporter(name, timer)
	return timer
}

// Size
// Return size of the duration set
func (this *Timer) Size() int {
	return len(this.duration)
}

// AddDuration
// Add a duration value to the set of durations
func (this *Timer) AddDuration(duration time.Duration) {
	// avoid blocking the caller
	// Note this will require a context switch before the
	// avg-duration function returns the new value
	go func() {
		// Note that a mutex is used over a channel to avoid
		// a continuously running goroutine for each timer
		this.lock.Lock()
		defer this.lock.Unlock()
		this.duration[this.current] = duration
		if this.sparse && this.current+1 == len(this.duration) {
			this.sparse = false
		}
		this.current = (this.current + 1) % len(this.duration)
	}()
}

// AvgDuration
// Calculate the average duration from the set of
// duration values (i.e., ramped up to a max set length, e.g.,
// if only two samples were taken, the result will be the average
// of those two samples - not the maximum number of samples allowed)
//
// Note that this read is not syncronized by design, do not add Rlock
func (this *Timer) AvgDuration() time.Duration {
	sum := time.Duration(0)
	size := 0
	if this.sparse {
		size = this.current
	} else {
		size = len(this.duration)
	}
	for i := 0; i < size; i++ {
		sum += this.duration[i]
	}
	avg := time.Duration(0)
	if size != 0 {
		avg = sum / time.Duration(size)
	}
	return avg
}

func (this *Timer) WriteTo(w io.Writer) {
	if this.template == "" {
		fmt.Fprintf(w, "\ttimer, %v = %v\n", this.name, this.AvgDuration())
	} else {
		fmt.Fprintf(w, this.template, this.AvgDuration())
	}
}

func (this *Timer) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":    "timer",
		"average": this.AvgDuration(),
		"size":    len(this.duration),
		"sparse":  this.sparse,
	})
}

// //////////////////////////////////////////////////////////////////////////
// Default Rate Reporter
// A Rate Reporter is used to record the average event frequency
//
// Example:
// If the user wanted to measure the number of web request per second,
//
//    profile := management.NewProfile("my_path.my_package")
//    myRate := profile.AddRate("my_rater", 10) // take the average of 10 samples
//
// Then, when the call is being measured,
//
//    myRate.AddSample()

type Rate struct {
	name, template string
	lock           sync.RWMutex
	samples        []time.Time
	sparse         bool
	current        int
}

// AddRate
// Add a Rate Reporter to the Profile
func (this *Profile) AddRate(name string, size int) *Rate {
	return this.AddRateWithTemplate(name, "", size)
}

// AddRateWithTemplate
// Add a Rate Reporter with a custom reporting template string
func (this *Profile) AddRateWithTemplate(name, template string, size int) *Rate {
	if size == 0 {
		return nil
	}
	rate := new(Rate)
	rate.name = name
	rate.template = template
	rate.samples = make([]time.Time, size+1)
	rate.current = 0
	rate.sparse = true
	this.AddReporter(name, rate)
	return rate
}

// Size
// Return size of the sample set
func (this *Rate) Size() int {
	return len(this.samples)
}

// AddSample
// Add time-stamp to the set of samples
func (this *Rate) AddSample(sample time.Time) {
	// avoid blocking the caller
	// Note this will require a context switch before the
	// avg-rate function returns the new value
	go func() {
		// Note that a mutex is used over a channel to avoid
		// a continuously running goroutine for each timer
		this.lock.Lock()
		defer this.lock.Unlock()
		this.samples[this.current] = sample
		if this.sparse && this.current+1 == len(this.samples) {
			this.sparse = false
		}
		this.current = (this.current + 1) % len(this.samples)
	}()
}

// AvgRate
// Determine and return the event frequency rate based on the
// samples provided
func (this *Rate) AvgRate() time.Duration {
	// return immediately, if possible
	avg := time.Duration(0)
	if this.sparse && this.current == 0 {
		return avg
	}

	// Unfortunetly, we must Rlock in this case since any
	// intervening add-sample can result in an incorrect
	// avg-rate calculation.
	this.lock.RLock()
	defer this.lock.RUnlock()

	// Note this assumes we dont need to sort the list first
	// (i.e., there are no out-of-order samples)
	sum := time.Duration(0)
	start := 0
	size := 0
	if this.sparse {
		start = 0
		size = this.current - 1
	} else {
		start = this.current
		size = len(this.samples) - 1
	}
	for i := start; i < start+size; i++ {
		j := i % len(this.samples)
		k := (i + 1) % len(this.samples)
		sum += this.samples[k].Sub(this.samples[j])
	}
	if size != 0 {
		avg = sum / time.Duration(size)
	}
	return avg
}

func (this *Rate) WriteTo(w io.Writer) {
	if this.template == "" {
		fmt.Fprintf(w, "\trate, %v = %v\n", this.name, this.AvgRate())
	} else {
		fmt.Fprintf(w, this.template, this.AvgRate())
	}
}

func (this *Rate) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":    "rate",
		"average": this.AvgRate(),
		"size":    len(this.samples),
		"sparse":  this.sparse,
	})
}
