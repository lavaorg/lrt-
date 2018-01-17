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

package rlimit

import (
	"fmt"
	"io/ioutil"
	"runtime"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/rte/config"
	"strconv"
	"strings"
	"time"
)

const (
	ERR_OUT_OF_MEMORY        = "out of memory"
	MEMORY_BUFFER_PERCENTAGE = 20
	MEMORY_LIMIT_IN_BYTES    = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	MEMORY_USAGE_IN_BYTES    = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
)

type ResourceLimit interface {
	StartMonitoring(resources *Resources) (chan error, error)
	StopMonitoring()
}

type Resources struct {
	Memory uint64
}

type CGroupMemoryStats struct {
	Usage uint64
	Limit uint64
}

type LuaResourceLimit struct {
	Done chan bool
	Msg  chan error
}

func NewLuaResourceLimit() *LuaResourceLimit {
	return &LuaResourceLimit{Done: make(chan bool),
		Msg: make(chan error)}
}

func (r *LuaResourceLimit) StartMonitoring(resources *Resources) (chan error, error) {
	setNumberOfThreads(config.GoMaxProcs)
	err := r.enforceMemoryLimit(resources)
	if err != nil {
		return nil, err
	}

	return r.Msg, nil
}

func (r *LuaResourceLimit) StopMonitoring() {
	if config.EnableRLimit {
		mlog.Debug("Sent stop message")
		r.Done <- true
		mlog.Debug("Stop message sent")
	}
}

func (r *LuaResourceLimit) enforceMemoryLimit(resources *Resources) error {
	var memLimit uint64
	if resources.Memory > 0 {
		ok, err := isEnoughMemoryAvailable(resources.Memory)
		if !ok {
			return err
		}
		memLimit = resources.Memory
	} else {
		memStats, err := getCGroupMemoryStats()
		if err != nil {
			return err
		}

		buffer := getPercentage(memStats.Limit, MEMORY_BUFFER_PERCENTAGE)
		memLimit = memStats.Limit - buffer - memStats.Usage
		mlog.Debug("System memory limit: %v, buffer: %v, usage %v",
			memStats.Limit, buffer, memStats.Usage)
	}

	r.monitorMemory(memLimit)
	return nil
}

func (r *LuaResourceLimit) monitorMemory(memory uint64) {
	mlog.Debug("Enforcing memory limit of %v", memory)
	MemLimit.Set(int64(memory))
	var initialStats runtime.MemStats
	runtime.ReadMemStats(&initialStats)

	go func() {
	L:
		for {
			select {
			case <-r.Done:
				mlog.Debug("Received monitoring stop signal")
				break L
			default:
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				updatePerformanceCounters(&memStats)
				allocated := getAllocatedMemory(memStats.Alloc, initialStats.Alloc)
				mlog.Debug("Allocated memory: %v, full memory stats: %v", allocated, memStats)
				if allocated >= memory {
					mlog.Debug("Sending out of memory message to channel")
					r.Msg <- fmt.Errorf(ERR_OUT_OF_MEMORY)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		mlog.Debug("Memory monitoring stopped")
	}()
}

func getAllocatedMemory(alloc, init uint64) uint64 {
	if alloc > init {
		return alloc - init
	}

	return alloc
}

func updatePerformanceCounters(mem *runtime.MemStats) {
	MemSys.Set(int64(mem.Sys))
	MemAlloc.Set(int64(mem.Alloc))
	MemTotalAlloc.Set(int64(mem.TotalAlloc))
	MemHeapAlloc.Set(int64(mem.HeapAlloc))
	MemHeapSys.Set(int64(mem.HeapSys))
	MemHeapObjects.Set(int64(mem.HeapObjects))
	MemHeapReleased.Set(int64(mem.HeapReleased))
	MemNumGC.Set(int64(mem.NumGC))
}

func isEnoughMemoryAvailable(memory uint64) (bool, error) {
	memStats, err := getCGroupMemoryStats()
	if err != nil {
		return false, err
	}

	mlog.Debug("Memory requested %v, used %v, limit %v", memory, memStats.Usage, memStats.Limit)

	reqMemory := memory + memStats.Usage
	if reqMemory > memStats.Limit {
		return false, fmt.Errorf("not enough memory available")
	}

	return true, nil
}

func getCGroupMemoryStats() (*CGroupMemoryStats, error) {
	usage, err := getValue(MEMORY_USAGE_IN_BYTES)
	if err != nil {
		return nil, err
	}

	limit, err := getValue(MEMORY_LIMIT_IN_BYTES)
	if err != nil {
		return nil, err
	}

	return &CGroupMemoryStats{Usage: usage, Limit: limit}, nil
}

func getPercentage(number uint64, percent uint64) uint64 {
	return uint64((float64(number) * float64(percent)) / float64(100))
}

func getValue(path string) (uint64, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}

	memStr := strings.Split(string(contents), "\n")
	memory, err := strconv.ParseUint(memStr[0], 0, 64)
	if err != nil {
		return 0, err
	}

	return memory, nil
}

func setNumberOfThreads(threads int) {
	mlog.Debug("Number of threads set to %d", threads)
	runtime.GOMAXPROCS(threads)
}
