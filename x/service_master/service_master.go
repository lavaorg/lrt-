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

package service_master

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// ServiceMaster implements a work pool with the specified concurrency level and queue capacity.
type ServiceMaster struct {
	shutdownWorkChannel chan string    // Channel used to shut down the work routines.
	shutdownWaitGroup   sync.WaitGroup // The WaitGroup for shutting down existing routines.
	workChannel         chan Worker    // Channel used to process work.
	queuedWork          int32          // The number of work items queued.
	activeRoutines      int32          // The number of routines active.
}

// New creates a new ServiceMaster.
func New(numberOfRoutines int, queueCapacity int) *ServiceMaster {
	svcMstr := ServiceMaster{
		shutdownWorkChannel: make(chan string),
		workChannel:         make(chan Worker, queueCapacity),
		queuedWork:          0,
		activeRoutines:      0,
	}
	// Add the total number of routines to the wait group
	svcMstr.shutdownWaitGroup.Add(numberOfRoutines)
	// Launch the work routines to process work
	for workID := 0; workID < numberOfRoutines; workID++ {
		go svcMstr.workRoutine(workID)
	}

	return &svcMstr
}

// Shutdown will release resources and shutdown all processing.
func (svcMstr *ServiceMaster) Shutdown(goRoutine string) (err error) {
	defer handlePanic(&err, goRoutine, "Shutdown")
	print(goRoutine, "Shutdown", "Started")
	print(goRoutine, "Shutdown", "Shutting Down Work Routines")
	close(svcMstr.shutdownWorkChannel)
	svcMstr.shutdownWaitGroup.Wait()
	close(svcMstr.workChannel)
	print(goRoutine, "Shutdown", "Completed")
	return err
}

// Dispatch will post work into the ServiceMaster.
func (svcMstr *ServiceMaster) Dispatch(goRoutine string, work Worker) (err error) {
	defer handlePanic(&err, goRoutine, "Dispatch")
	svcMstr.workChannel <- work
	return err
}

// QueuedWork will return the number of work items in queue.
func (svcMstr *ServiceMaster) QueuedWork() int32 {
	return atomic.AddInt32(&svcMstr.queuedWork, 0)
}

// ActiveRoutines will return the number of routines performing work.
func (svcMstr *ServiceMaster) ActiveRoutines() int32 {
	return atomic.LoadInt32(&svcMstr.activeRoutines)
}

// HandlePanic is used to catch any Panic and log exceptions to Stdout. It will also write the stack trace.
func handlePanic(err *error, goRoutine string, functionName string) {
	if r := recover(); r != nil {
		// Capture the stack trace
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)
		printf(goRoutine, functionName, "PANIC Defered [%v] : Stack Trace : %v", r, string(buf))
		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}

// print is used to write a system message directly to stdout.
func print(goRoutine string, functionName string, message string) {
	mlog.Emit(mlog.DEBUG, fmt.Sprintf("%s : %s : %s\n", goRoutine, functionName, message))
}

// printf is used to write a formatted system message directly stdout.
func printf(goRoutine string, functionName string, format string, a ...interface{}) {
	print(goRoutine, functionName, fmt.Sprintf(format, a...))
}

// workRoutine performs the work required by the work pool
func (svcMstr *ServiceMaster) workRoutine(workRoutine int) {
	for {
		select {
		// Shutdown the WorkRoutine.
		case <-svcMstr.shutdownWorkChannel:
			print(fmt.Sprintf("WorkRoutine %d", workRoutine), "workRoutine", "Going Down")
			svcMstr.shutdownWaitGroup.Done()
			return
		// There is work in the queue.
		case poolWorker := <-svcMstr.workChannel:
			atomic.AddInt32(&svcMstr.queuedWork, 1)
			err := svcMstr.run(workRoutine, poolWorker)
			if err != nil {
				print(fmt.Sprintf("Error %d", workRoutine), "Service Master :", err.Error())
			}
			break
		}
	}
}

// run executes the user Run method.
func (svcMstr *ServiceMaster) run(workRoutine int, poolWorker Worker) (err error) {
	defer handlePanic(nil, "WorkRoutine", "run")
	defer atomic.AddInt32(&svcMstr.activeRoutines, -1)
	// Update the counts
	atomic.AddInt32(&svcMstr.queuedWork, -1)
	atomic.AddInt32(&svcMstr.activeRoutines, 1)
	// Perform the work
	err = poolWorker.Run(workRoutine)
	return err

}
