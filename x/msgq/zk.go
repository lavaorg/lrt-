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

package msgq

import (
	"github.com/verizonlabs/northstar/pkg/zklib"
	"strconv"
)

// Commit commits an offset to a service/topic/partition
func CommitOffset(Zk *zklib.ZK, node string, offset int64) (err error) {
	data := []byte(strconv.FormatInt(offset, 10))
	return Zk.WriteData(node, data, false)
}
