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

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/zklib"
)

func getArgs() (string, string, string) {
	servers := flag.String("s", "127.0.0.1:2181", "Comma separated list of servers, including ports. For ex: -s='127.0.0.1:2181'")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Println("Usage: ")
		fmt.Println("    go run zksubscribe.go [-s=<zkserver:port>] [-c] path data")
		os.Exit(0)
	}
	zkpath := flag.Arg(0)
	data := flag.Arg(1)
	return *servers, zkpath, data
}

var gRootNodePath string

func main() {
	servers, zkpath, data := getArgs()

	zk, err := zklib.NewZK(strings.Split(servers, ","), 1e9)
	if err != nil {
		fmt.Println("Failed to connect to zookeeper", err)
		panic("Zookeeper server not reachable")
	}
	mlog.Debug("Creating", zkpath, "with initial data", data)

	gRootNodePath = zkpath

	if evt, data, err := zk.Subscribe(zkpath, true); err != nil {
		return
	} else {
		UpdateSetRouterTableInfo(*data)
		handleZkEvent(evt)
	}
	return

}

// This function will handle all the indication from Zookeeper
func handleZkEvent(event <-chan zklib.SubEventData) {
	var ok bool = true
	var val zklib.SubEventData
	mlog.Debug("handleZkEvent() starts")

	for ok {
		select {
		case val, ok = <-event:
			if ok && val.SubPath == gRootNodePath {
				mlog.Debug("====> new mapping received FROM Zookeeper")
				UpdateSetRouterTableInfo(val)
			} else {
				mlog.Error("ok[%v] Path[%v]", ok, val.SubPath)
			}
		}

	}

	mlog.Debug("handleZkEvent() Ends")
	return
}

// This function will update the data structure to keep the data in in-memory
func UpdateSetRouterTableInfo(compInfo zklib.SubEventData) (err error) {
	mlog.Debug("UpdateSetRouterTableInfo() starts")

	mlog.Debug("UpdateSetRouterTableInfo: len(compInfo)=%d, compInfo=%v", len(compInfo.SubData), compInfo)
	if len(compInfo.SubData) == 0 {
		mlog.Debug("Empty data from Zookeeper")
	} else {

		for path, val := range compInfo.SubData {
			mlog.Debug("Path=", path, "val=", val)
		}
	}
	mlog.Debug("UpdateSetRouterTableInfo() ends")
	return nil
}
