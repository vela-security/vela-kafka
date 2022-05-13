package producer

import (
	"github.com/vela-security/vela-public/assert"
	"github.com/vela-security/vela-public/lua"
	"reflect"
)

var (
	xEnv   assert.Environment
	typeof = reflect.TypeOf((*Producer)(nil)).String()
)

func (p *Producer) LPush(L *lua.LState) int {

	n := L.GetTop()
	if n <= 0 {
		return 0
	}

	for i := 1; i <= n; i++ {
		v := L.Get(i)
		p.Push(v.String())
	}
	return 0
}

func (p *Producer) LTotal(L *lua.LState) int {
	v := p.Total()
	L.Push(lua.LNumber(v))
	return 1
}

func (p *Producer) newSDK(L *lua.LState) int {
	topic := L.CheckString(1)
	ud := lua.NewProcData(newSDKL(p, topic))
	L.Push(ud)
	return 1
}

func (p *Producer) startL(L *lua.LState) int {
	xEnv.Start(L, p).From(p.CodeVM()).Do()
	return 0
}

func (p *Producer) Index(L *lua.LState, key string) lua.LValue {

	switch key {

	case "push":
		return L.NewFunction(p.LPush)

	case "total":
		return L.NewFunction(p.LTotal)

	case "sdk":
		return L.NewFunction(p.newSDK)

	case "start":
		return L.NewFunction(p.startL)
	}

	return lua.LNil
}

func (p *Producer) NewIndex(L *lua.LState, key string, val lua.LValue) {
	if key == "thread" {
		p.cfg.thread = lua.CheckInt(L, val)
	}
}

func newLuaProducer(L *lua.LState) int {
	cfg := newConfig(L)
	proc := L.NewProc(cfg.name, typeof)
	if proc.IsNil() {
		proc.Set(newProducer(cfg))
	} else {
		proc.Data.(*Producer).cfg = cfg
	}

	L.Push(proc)
	return 1
}

func WithEnv(env assert.Environment, uv lua.UserKV) {
	xEnv = env
	uv.Set("producer", lua.NewFunction(newLuaProducer))
}
