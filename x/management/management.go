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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/verizonlabs/northstar/pkg/mlog"
)

var (
	HealthLogRate  = 1
	healthLogCount = 1
	ProfilingEnabled   = true
)

// initialize basic management services

func init() {
	//if client does not want any restend point expose,
	// client can achive this by doing following
	if ep := os.Getenv("MGMT_HTTP_SERVER_DISABLED"); ep != "" {
		if ep == "true" {
			log.Debug("NO REST endpoint exposed, MGMT_HTTP_SERVER_DISABLED ")
			return
		}
	}
	exposeRestEndPoint()
}

func exposeRestEndPoint() {

	// initialize management variables
	if hlr := os.Getenv("MAN_HEALTHLOG_RATE"); hlr != "" {
		HealthLogRate, _ = strconv.Atoi(hlr)
		healthLogCount = HealthLogRate
	}
	if dbg := os.Getenv("MAN_PROFILING_ENABLED"); dbg != "" {
		if value, err := strconv.ParseBool(dbg); err == nil {
			ProfilingEnabled = value
		}
	}

	// initialize management group
	mn := Engine().Group("/management")

	// add health handler
	mn.GET("/health", getHealth)
	mn.PUT("/log/debug/:enable", toggleDebug)

	// add profile handler
	mn.GET("/profile/:name", getProfile)
	mn.GET("/report/:name", getReport)

	// add pprof handlers, see http://blog.golang.org/profiling-go-programs
	// e.g., go tool pprof http://localhost:8888/management/debug/pprof/<profile>
	//       go tool pprof <binary> <profile>
	// profiles are incomplete and inaccurate on NetBSD and OS X.
	// see http://golang.org/issue/6047 for details.
	mn.GET("/debug/pprof/:name", getPprof)
	mn.POST("/debug/pprof/symbol", getOrPostPprofSymbol)
}

// http handler get health
// GET /management/health
func getHealth(context *gin.Context) {

	// return the current health status
	context.JSON(health.HttpStatus, health)
}

// http handler get profile
// GET /management/profile/:name
func getProfile(context *gin.Context) {

	name := context.Params.ByName("name")
	if name == "names" {
		getProfileNames(context)
		return
	}

	// Do not add read locks to getProfile
	//   profileLock.RLock()
	//   defer profileLock.Unlock()
	profile, found := profiles[name]
	if !found {
		context.String(http.StatusNotFound, "profile not found, %v", name)
		return
	}
	context.JSON(http.StatusOK, profile)
}

// http handler get profile report
// GET /management/report/:name
func getReport(context *gin.Context) {

	name := context.Params.ByName("name")
	if name == "names" {
		getProfileNames(context)
		return
	}

	// Do not add read locks to getProfile
	//   profileLock.RLock()
	//   defer profileLock.Unlock()
	profile, found := profiles[name]
	if !found {
		context.String(http.StatusNotFound, "profile not found, %v", name)
		return
	}
	context.Writer.WriteHeader(http.StatusOK)
	context.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	profile.WriteTo(context.Writer)
}

// http handler to get list of available profile names
// GET /management/profile/names
// GET /management/report/names
func getProfileNames(context *gin.Context) {
	names := make([]string, 0)
	for key := range profiles {
		names = append(names, key)
	}
	context.String(http.StatusOK, strings.Join(names, ","))

}

// getPprof returns the named package profile.
// e.g., go tool pprof http://localhost:8888/management/debug/pprof/<profile>
// profiles are incomplete and inaccurate on NetBSD and OS X.
// see http://golang.org/issue/6047 for details.
// adapted from Go 1.3, https://golang.org/src/net/http/pprof/pprof.go, accessed April 2015
//
// the names may be one of the following,
//		names		- return the list of profiles
//		symbol		- symbol name from a pc
//		cmdline		- application command line
//		profile		- cpu profile
//
// the following is reprinted from runtime/pprof/pprof.go,
//
// 	"A Profile is a collection of stack traces showing the call sequences
// 	that led to instances of a particular event, such as allocation.
// 	Packages can create and maintain their own profiles; the most common
// 	use is for tracking resources that must be explicitly closed, such as files
// 	or network connections.
//
// 	A Profile's methods can be called from multiple goroutines simultaneously.
//
// 	Each Profile has a unique name.  A few profiles are predefined:
//
//		goroutine    - stack traces of all current goroutines
//		heap         - a sampling of all heap allocations
//		threadcreate - stack traces that led to the creation of new OS threads
//		block        - stack traces that led to blocking on synchronization primitives
//
// 	These predefined profiles maintain themselves and panic on an explicit
//	Add or Remove method call."
//
// GET /management/debug/pprof/:name
func getPprof(context *gin.Context) {
	// ignore if not enabled
	if !ProfilingEnabled {
		context.String(http.StatusNotFound, "Profiling not enabled")
		return
	}
	// pull name and execute
	name := context.Params.ByName("name")
	switch name {
	case "names":
		getPprofNames(context)
	case "symbol":
		getOrPostPprofSymbol(context)
	case "cmdline":
		getPprofCmdline(context)
	case "profile":
		getCprof(context)
	case "trace":
		getTprof(context)
	default:
		p := pprof.Lookup(name)
		if p == nil {
			context.String(http.StatusNotFound, "debug profile not found, %s", name)
		} else {
			debug := 1
			context.Writer.WriteHeader(http.StatusOK)
			context.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			p.WriteTo(context.Writer, debug)
		}
	}
}

// getPprofNames returns all available package profiles
// GET /management/debug/pprof/names
func getPprofNames(context *gin.Context) {
	profiles := pprof.Profiles()
	names := make([]string, 0)
	for _, profile := range profiles {
		names = append(names, profile.Name())
	}
	context.String(http.StatusOK, strings.Join(names, ","))
}

// getOrPostSymbol looks up the program counters listed in the request,
// responding with a table mapping program counters to function names.
// adapted from Go 1.3, https://golang.org/src/net/http/pprof/pprof.go, accessed April 2015
// GET /management/debug/pprof/symbol
// POST /management/debug/pprof/symbol
func getOrPostPprofSymbol(context *gin.Context) {
	w := context.Writer
	r := context.Request

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "num_symbols: 1\n")
	var b *bufio.Reader
	if r.Method == "POST" {
		b = bufio.NewReader(r.Body)
	} else {
		b = bufio.NewReader(strings.NewReader(r.URL.RawQuery))
	}
	for {
		word, err := b.ReadSlice('+')
		if err == nil {
			word = word[0 : len(word)-1] // trim +
		}
		pc, _ := strconv.ParseUint(string(word), 0, 64)
		if pc != 0 {
			f := runtime.FuncForPC(uintptr(pc))
			if f != nil {
				fmt.Fprintf(&buf, "%#x %s\n", pc, f.Name())
			}
		}
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(&buf, "reading request: %v\n", err)
			}
			break
		}
	}
	w.Write(buf.Bytes())
}

// getCmdline responds with the running program's command line
// adapted from Go 1.3, https://golang.org/src/net/http/pprof/pprof.go, accessed April 2015
// GET /management/debug/pprof/cmdline
func getPprofCmdline(context *gin.Context) {
	context.String(http.StatusOK, strings.Join(os.Args, "\x00"))
}

// getProfile performs cpu profiling for the given
// duration, where the default profile duration is 30 seconds.
// adapted from Go 1.3, https://golang.org/src/net/http/pprof/pprof.go, accessed April 2015
// GET /management/debug/pprof/profile?seconds
func getCprof(context *gin.Context) {
	context.Request.ParseForm()
	duration := context.Request.Form.Get("seconds")
	durationx, _ := strconv.ParseInt(duration, 10, 64)
	if durationx == 0 {
		durationx = 30
	}
	context.Writer.WriteHeader(http.StatusOK)
	context.Writer.Header().Set("Content-Type", "application/octet-stream")
	if err := pprof.StartCPUProfile(context.Writer); err != nil {
		context.String(http.StatusInternalServerError, "profiling failed, %v", err)
		return
	}
	time.Sleep(time.Duration(durationx) * time.Second)
	pprof.StopCPUProfile()
}

// execution tracer profile
func getTprof(context *gin.Context) {
	context.Request.ParseForm()
	duration := context.Request.Form.Get("seconds")
	durationx, _ := strconv.ParseInt(duration, 10, 64)
	if durationx == 0 {
		durationx = 5
	}
	context.Writer.WriteHeader(http.StatusOK)
	context.Writer.Header().Set("Content-Type", "application/octet-stream")
	if err := trace.Start(context.Writer); err != nil {
		context.String(http.StatusInternalServerError, "trace Start failed, %v", err)
		return
	}
	time.Sleep(time.Duration(durationx) * time.Second)
	trace.Stop()
}

func toggleDebug(context *gin.Context) {
	enableDebug := context.Params.ByName("enable")
	booleanValue, err := strconv.ParseBool(enableDebug)
	if err != nil {
		context.String(http.StatusInternalServerError, "Invalid boolean value passed, %v", err)
		return
	}
	log.EnableDebug(booleanValue)
}
