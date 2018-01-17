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
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"regexp"
	"strings"
)

var paths []string

func getArgs() (*bool, *string, string) {
	data := flag.Bool("d", false, "Print data associated with each path")
	servers := flag.String("s", "127.0.0.1:11108", "Comma separated list of servers, including ports. For ex: -servers='127.0.0.1:2181'")
	re := flag.String("r", "*", "Regex to filter the paths")
	flag.Parse()
	regex := strings.Replace(*re, "*", ".*", -1)
	return data, servers, regex
}

func getPaths(conn *zk.Conn, root string) {
	paths = append(paths, root)
	children, _, err := conn.Children(root)
	if err != nil {
		panic(err)
	}

	if len(children) == 0 {
		return
	}

	for idx := range children {
		getPaths(conn, path.Join(root, children[idx]))
	}
}

func main() {
	data, servers_arg, regex := getArgs()

	zk_servers := []string(strings.Split(*servers_arg, ","))
	conn, _, err := zk.Connect(zk_servers, 1e9)
	if err != nil {
		panic("Zookeeper server not reachable")
	}
	getPaths(conn, "/")

	for x := range paths {
		matched, _ := regexp.MatchString(regex, paths[x])
		if matched {
			fmt.Println(paths[x])
			if *data {
				d, stat, err := conn.Get(paths[x])
				if err != nil {
					fmt.Println(err)
				} else {
					//fmt.Println(stat.DataLength)
					//fmt.Println(d)
					s := string(d[:stat.DataLength])
					fmt.Printf("%q\n", s)
				}
			}
		}
	}
}
