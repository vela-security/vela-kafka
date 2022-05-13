package producer

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/vela-security/vela-public/auxlib"
	"github.com/vela-security/vela-public/kind"
	"github.com/vela-security/vela-public/lua"
	"sync/atomic"
	"time"
)

type Producer struct {
	lua.ProcEx
	cfg *config

	send   uint64
	recv   uint64
	buff   chan message
	ctx    context.Context
	cancel context.CancelFunc

	thread  []*Thread
	limiter *Limiter

	topic  string
	uptime time.Time
}

func newProducer(cfg *config) *Producer {
	return &Producer{cfg: cfg}
}

func (p *Producer) Name() string {
	return p.cfg.name
}

func (p *Producer) Type() string {
	return "kafka.producer"
}

func (p *Producer) CodeVM() string {
	return p.cfg.co.CodeVM()
}

func (p *Producer) Total() uint64 {
	var v uint64
	for i := 0; i < p.cfg.thread; i++ {
		atomic.AddUint64(&v, p.thread[i].total)
	}
	return v
}

func (p *Producer) Write(v []byte) (int, error) {
	if !p.IsRun() {
		return 0, nil
	}

	n := len(v)
	if n <= 0 {
		return 0, nil
	}

	p.buff <- message{p.cfg.topic, v}
	atomic.AddUint64(&p.recv, uint64(n))

	return n, nil
}

func (p *Producer) Push(v interface{}) {
	if !p.IsRun() {
		return
	}

	auxlib.Push(p, v)
}

func (p *Producer) Close() error {
	p.V(lua.PTClose)
	close(p.buff)
	p.cancel()
	return nil
}

// 开始传输
func (p *Producer) Start() error {

	p.buff = make(chan message, p.cfg.buffer)
	p.limiter = newLimiter(p.cfg.limit)

	//创建并启动程序
	p.thread = make([]*Thread, p.cfg.thread)
	p.ctx, p.cancel = context.WithCancel(context.Background())
	for i := 0; i < p.cfg.thread; i++ {
		p.thread[i] = newProducerThread(p.ctx, i, p.cfg, p.limiter, p.buff)
		go p.thread[i].start() //启动线程
	}

	go p.Heartbeat()

	return nil
}

func (p *Producer) Ping() {
	for id, t := range p.thread {
		switch t.status {
		case OK:
			//xEnv.Errorf("%s kafka thread.idx = %d up" , t.cfg.name , id)
			continue

		case CLOSE:
			xEnv.Errorf("%s kafka thread.idx = %d close", t.cfg.name, id)
			//pub.Out.Err("%s kafka threads check: topic [%s], %d up, %d down", p.cfg.name , p.cfg.topic, p.count, p.cfg.thread-p.count)
		case ERROR:
			go p.thread[id].start()
			//pub.Out.Err("%s kafka thread.idx = %d start" , p.cfg.name , idx)
		}
	}

}

// 心跳检测
func (p *Producer) Heartbeat() {
	tk := time.NewTicker(time.Second * time.Duration(p.cfg.heartbeat))
	defer tk.Stop()

	for {
		select {
		case <-p.ctx.Done():
			xEnv.Errorf("%s kafka heartbeat exit", p.cfg.name)
			return
		case <-tk.C:
			p.Ping()
		}
	}
}

func (p *Producer) ToJson() ([]byte, error) {
	buff := kind.NewJsonEncoder()
	buff.Tab("kafka")
	buff.KV("type", "producer")
	buff.KV("name", p.cfg.name)
	buff.KV("addr", p.cfg.addr)
	buff.KV("topic", p.cfg.topic)
	buff.KI("num", p.cfg.num)
	buff.KI("flush", p.cfg.flush)
	buff.KI("thread", p.cfg.thread)
	buff.KI("limit", p.cfg.limit)
	buff.KI("heartbeat", p.cfg.heartbeat)
	buff.KI("timeout", p.cfg.timeout)
	buff.KV("compression", sarama.CompressionCodec(p.cfg.compression).String())
	buff.End("}")
	return buff.Bytes(), nil
}
