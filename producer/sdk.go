package producer

import (
	"github.com/vela-security/vela-public/auxlib"
	"github.com/vela-security/vela-public/lua"
	"sync/atomic"
)

type sdk struct {
	lua.ProcEx

	object *Producer
	topic  string
	recv   uint64
}

/*
	Name()   string //获取当前对象名称
	Type()   string //获取对象类型
	State()  ProcState //获取状态
	Start()  error
	Close()  error
	SetField(*LState, LValue, LValue) //设置字段
	GetField(*LState, LValue) LValue  //获取字段
	Index(*LState, string) LValue     //获取字符串字段 __index function
	NewIndex(*LState, string, LValue) //设置字段 __newindex function
*/

func newSDKL(p *Producer, topic string) *sdk {
	return &sdk{object: p, topic: topic}
}

func (s *sdk) ToLValue() lua.LValue {
	return lua.NewProcData(s)
}

func (s *sdk) Name() string {
	return s.object.Name()
}

func (s *sdk) Type() string {
	return "kafka.producer.sdk"
}

func (s *sdk) State() lua.ProcState {
	return s.object.Status
}

func (s *sdk) Start() error {
	return nil
}

func (s *sdk) Close() error {
	return nil
}

func (s *sdk) Write(v []byte) (int, error) {
	if !s.object.IsRun() {
		return 0, nil
	}

	n := len(v)
	if n <= 0 {
		return 0, nil
	}

	s.object.buff <- message{s.topic, v}

	atomic.AddUint64(&s.recv, uint64(n))
	return n, nil
}

func (s *sdk) pushL(L *lua.LState) int {

	n := L.GetTop()
	if n <= 0 {
		return 0
	}

	for i := 1; i <= n; i++ {
		v := L.Get(i)
		s.Write(auxlib.S2B(v.String()))
	}
	return 0
}

func (s *sdk) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "push":
		return L.NewFunction(s.pushL)
	case "total":
		return lua.LNumber(s.recv)
	}
	return lua.LNil
}
