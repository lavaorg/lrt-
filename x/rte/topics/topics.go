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

package topics

import (
	"fmt"
	"github.com/verizonlabs/northstar/pkg/rte/repl"
)

func GetCtrlTopicByType(rteType string) (string, error) {
	switch rteType {
	case repl.R:
		return RTE_R_CTRL_TOPIC, nil
	case repl.Lua:
		return RTE_LUA_CTRL_TOPIC, nil
	default:
		return "", fmt.Errorf("Wrong RTE type specified: %s", rteType)
	}
}
