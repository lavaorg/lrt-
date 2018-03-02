package luaextended

import "github.com/lavaorg/lua"

const (
	// JsonLibName is the name of the coroutine Library.
	JsonLibName = "json"
	// StringsLibName is the name of the coroutine Library.
	StringsLibName = "strings"
)

type luaExtLib struct {
	libName string
	libFunc lua.LGFunction
}

var luaExtLibs = []luaExtLib{
	luaExtLib{StringsLibName, OpenStrings},
	luaExtLib{JsonLibName, OpenJson},
}

// OpenLibs loads the built-in libraries. It is equivalent to running OpenLoad,
// then OpenBase, then iterating over the other OpenXXX functions in any order.
func OpenLibs(ls *lua.LState) {
	// NB: Map iteration order in Go is deliberately randomised, so must open Load/Base
	// prior to iterating.
	for _, lib := range luaExtLibs {
		ls.Push(ls.NewFunction(lib.libFunc))
		ls.Push(lua.LString(lib.libName))
		ls.Call(1, 0)
	}
}
