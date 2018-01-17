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
	"github.com/verizonlabs/northstar/pkg/zklib"
	"strings"
)

func getArgs() (bool, string, string, string) {
	create := flag.Bool("c", false, "Create the given path")
	servers := flag.String("s", "127.0.0.1:2181", "Comma separated list of servers, including ports. For ex: -s='127.0.0.1:2181'")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Println("Usage: ")
		fmt.Println("    go run zkput.go [-s=<zkserver:port>] [-c] path data")
		os.Exit(0)
	}
	zkpath := flag.Arg(0)
	data := flag.Arg(1)
	return *create, *servers, zkpath, data
}

func main() {
	create, servers, zkpath, data := getArgs()

	zk, err := zklib.GetZkInstance(strings.Split(servers, ","), 0)
	if err != nil {
		fmt.Println("Failed to connect to zookeeper", err)
		panic("Zookeeper server not reachable")
	}

	if !create {
		ok, err := zk.Exists(zkpath)
		if err != nil {
			fmt.Println("Failed to check if the path exists", err)
			return
		}

		if !ok {
			fmt.Println("Path", zkpath, "does not exist. Use -c option to create it before writing data")
			return
		}
	}

	err = zk.WriteData(zkpath, []byte(data), false)
	if err != nil {
		fmt.Println("Failed to write data to zookeeper", err)
		return
	}
}
