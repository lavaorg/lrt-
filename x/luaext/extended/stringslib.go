package luaextended

import (
	"github.com/lavaorg/lua"
	"strings"
)

func OpenStrings(L *lua.LState) int {
	mod := L.RegisterModule(StringsLibName, strsFuncs).(*lua.LTable)
	L.Push(mod)
	return 1
}

var strsFuncs = map[string]lua.LGFunction{
	"join":      strJoin,
	"split":     strSplit,
	"splitn":    strSplitN,
	"trim":      strTrim,
	"trimleft":  strTrimLeft,
	"trimrght":  strTrimRight,
	"trimspace": strTrimSpace,
}

func strJoin(L *lua.LState) int {
	strTbl := L.CheckTable(1)
	sep := L.CheckString(2)
	if strTbl == nil {
		L.Push(lua.LNil)
		return 1
	}
	strArr := make([]string, 0)
	for i := 1; i <= strTbl.Len(); i++ {
		strArr = append(strArr, strTbl.RawGetInt(i).String())
	}
	L.Push(lua.LString(strings.Join(strArr, sep)))
	return 1
}

func strSplit(L *lua.LState) int {
	str := L.CheckString(1)
	sep := L.CheckString(2)

	strArr := strings.Split(str, sep)
	arr := L.CreateTable(len(strArr), 0)
	for _, item := range strArr {
		arr.Append(lua.LString(item))
	}
	L.Push(arr)
	return 1
}

func strSplitN(L *lua.LState) int {
	str := L.CheckString(1)
	sep := L.CheckString(2)
	num := L.CheckInt(3)

	strArr := strings.SplitN(str, sep, num)
	arr := L.CreateTable(len(strArr), 0)
	for _, item := range strArr {
		arr.Append(lua.LString(item))
	}
	L.Push(arr)
	return 1
}

func strTrim(L *lua.LState) int {
	str := L.CheckString(1)
	cuteset := L.CheckString(2)
	L.Push(lua.LString(strings.Trim(str, cuteset)))
	return 1
}

func strTrimLeft(L *lua.LState) int {
	str := L.CheckString(1)
	cuteset := L.CheckString(2)
	L.Push(lua.LString(strings.TrimLeft(str, cuteset)))
	return 1
}

func strTrimRight(L *lua.LState) int {
	str := L.CheckString(1)
	cuteset := L.CheckString(2)
	L.Push(lua.LString(strings.TrimRight(str, cuteset)))
	return 1
}

func strTrimSpace(L *lua.LState) int {
	str := L.CheckString(1)
	L.Push(lua.LString(strings.TrimSpace(str)))
	return 1
}
