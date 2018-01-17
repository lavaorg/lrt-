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

package lualib

import (
	"github.com/yuin/gopher-lua"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

type mlogModule struct {
}

func (m *mlogModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"debug": m.logDebug,
		"info":  m.logInfo,
		"error": m.logError,
		"stat":  m.logStat,
		"event": m.logEvent,
		"alarm": m.logAlarm,
	})
	L.Push(mod)
	return 1
}

func NewMlogModule() *mlogModule {
	return &mlogModule{}
}

func (m *mlogModule) logDebug(L *lua.LState) int {
	if mlog.Severity() >= mlog.DEBUG {
		mlog.Debug(m.print(L))
	}
	return 0
}

func (m *mlogModule) logInfo(L *lua.LState) int {
	if mlog.Severity() >= mlog.INFO {
		mlog.Info(m.print(L))
	}
	return 0
}

func (m *mlogModule) logError(L *lua.LState) int {
	if mlog.Severity() >= mlog.INFO {
		mlog.Error(m.print(L))
	}
	return 0
}

func (m *mlogModule) logStat(L *lua.LState) int {
	if mlog.Severity() >= mlog.STAT {
		mlog.Stat(m.print(L))
	}
	return 0
}

func (m *mlogModule) logEvent(L *lua.LState) int {
	if mlog.Severity() >= mlog.EVENT {
		mlog.Event(m.print(L))
	}
	return 0
}

func (m *mlogModule) logAlarm(L *lua.LState) int {
	if mlog.Severity() >= mlog.ALARM {
		mlog.Alarm(m.print(L))
	}
	return 0
}

func (m *mlogModule) print(L *lua.LState) string {
	top := L.GetTop()
	outStr := (L.ToStringMeta(L.Get(1)).String())
	for i := 2; i <= top; i++ {
		outStr = outStr + L.ToStringMeta(L.Get(i)).String()
	}
	return outStr
}
