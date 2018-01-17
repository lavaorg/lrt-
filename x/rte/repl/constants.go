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

package repl

const (
	Lua = "lua"
	R   = "r"
)

const (
	STATE_CREATE_FAILED     = "STATE_CREATE_FAILED"
	SNIPPET_OUT_OF_MEMORY   = "OUT_OF_MEMORY"
	SNIPPET_CODE_GET_FAILED = "CODE_GET_FAILED"
	SNIPPET_REPL_FAILED     = "REPL_FAILED"
	SNIPPET_RUN_FINISHED    = "FINISHED"
	SNIPPET_RUN_TIMEDOUT    = "TIMED_OUT"
	START_MONITORING_FAILED = "START_MONITORING_FAILED"

	SNIPPET_RUN_TIMEDOUT_DESCR  = "snippet execution deadline exceeded"
	SNIPPET_OUT_OF_MEMORY_DESCR = "snippet has run out of memory"
)
