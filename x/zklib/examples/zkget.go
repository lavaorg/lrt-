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
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/zklib"
	"strconv"
	"strings"
)

func getArgs() (string, string, string) {
	servers := flag.String("s", "127.0.0.1:11108", "Comma separated list of servers, including ports. For ex: -s='127.0.0.1:2181'")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Println("Usage: ")
		fmt.Println("    go run zkput.go [-s=<zkserver:port>] [-c] path")
		os.Exit(0)
	}
	zkpath := flag.Arg(0)
	data := flag.Arg(1)
	return *servers, zkpath, data
}

func Bytes(hex string) (b []byte, err error) {
	if len(hex)%2 != 0 {
		mlog.Error("Error")
		return nil, err
	}
	octets := make([]byte, 0, len(hex)/2)
	for i := 0; i < len(hex); i += 2 {
		frame := hex[i : i+2]
		oct, err := strconv.ParseUint(frame, 16, 8)
		if err != nil {
			mlog.Error("Error")

			return nil, err
		}
		octets = append(octets, byte(oct))
	}
	return octets, nil
}

func main() {
	//mlog.EnableDebug(true)
	mlog.EnableDebug(false)
	servers, zkpath, data := getArgs()

	fmt.Println(servers, zkpath, data)
	zkp, err := zklib.NewZK(strings.Split(servers, ","), 1e9)
	if err != nil {
		fmt.Println("Failed to connect to zookeeper", err)
		panic("Zookeeper server not reachable")
	}

	data1, err := zkp.GetData(zkpath)
	if err != nil {
		fmt.Println("Failed to Get data for zookeeper", zkpath, err)
		return
	}

	fmt.Println(fmt.Sprintf("Data [%+v] [%s]", data1, data1))
}
