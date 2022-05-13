package producer

import (
	"context"
	"github.com/Shopify/sarama"
	"time"
)

// kafka 线程
type Thread struct {
	cfg    *config //配置文件
	idx    int     // 线程id
	status int     //状态

	count    uint32                    // 线程中待发送数据条数
	producer sarama.SyncProducer       //生产者
	Messages []*sarama.ProducerMessage //缓存当前消息
	buffer   chan message
	limiter  *Limiter
	ctx      context.Context
	total    uint64
}

func newProducerThread(ctx context.Context, idx int,
	cfg *config, limiter *Limiter, buff chan message) *Thread {

	th := &Thread{
		idx:      idx,
		cfg:      cfg,
		ctx:      ctx,
		count:    0,
		Messages: make([]*sarama.ProducerMessage, cfg.num),
		limiter:  limiter,
		status:   INIT,
		buffer:   buff,
	}

	for i := 0; i < cfg.num; i++ {
		th.Messages[i] = &sarama.ProducerMessage{Topic: cfg.topic}
	}
	return th
}

func (t *Thread) SendMessage() bool { //定时发送消息
	if t.count <= 0 {
		return true
	}

	if t.producer == nil {
		return false
	}

	var i uint32
	var msg *sarama.ProducerMessage

	rc := true
	for i = 0; i < t.count; i++ {
		msg = t.Messages[i]
		pt, offset, err := t.producer.SendMessage(msg)
		if err != nil {
			xEnv.Errorf("%s kafka thread.idx=%d send message err:%v", t.cfg.name, t.idx, err)
			rc = false
		} else {
			xEnv.Infof("%s kafka thread.idx=%d partition=%d offset=%d", t.cfg.name, t.idx, pt, offset)
			t.total++
		}
		msg.Value = nil
		msg.Topic = t.cfg.topic
	}

	t.count = 0
	return rc
}

func (t *Thread) Handler() {
	var chunk message
	var ok bool
	var msg *sarama.ProducerMessage

	tk := time.NewTicker(time.Second * time.Duration(t.cfg.flush))
	for {
		//获取当前限速令牌
		t.limiter.Handler(t.cfg.name, t.idx)
		select {

		case <-t.ctx.Done():
			t.SendMessage() //保证最后缓存数据不丢
			xEnv.Errorf("%s kafka thread.idx=%d exit successful", t.cfg.name, t.idx)
			t.close()
			return

		//读取缓存区
		case chunk, ok = <-t.buffer:
			if ok {
				msg = t.Messages[t.count]
				msg.Value = sarama.ByteEncoder(chunk.value)
				msg.Topic = chunk.topic

				t.Messages[t.count] = msg
				t.count++
				if t.count == uint32(t.cfg.num) {
					goto Send
				}

			} else {
				xEnv.Errorf("%s kafka thread.idx=%d buffer channel close", t.cfg.name, t.idx)
				t.close()
				return
			}

		//定时任务触发
		case <-tk.C:
			goto Send
		}

		//没有出发任何事件
		continue

		//发送数据
	Send:
		if !t.SendMessage() {
			t.status = ERROR //返回error heartbeat 重启
			return
		}
	}
}

func (t *Thread) close() {
	t.status = CLOSE

	//关闭client
	if err := t.producer.Close(); err != nil {
		xEnv.Errorf("%s thread.idx=%d close error: %v", t.cfg.name, t.idx, err)
	} else {
		xEnv.Errorf("%s thread.idx=%d close successful", t.cfg.name, t.idx)
	}
}

func (t *Thread) start() error {

	var err error
	c := sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Partitioner = sarama.NewRandomPartitioner
	c.Producer.Return.Successes = true

	t.producer, err = sarama.NewSyncProducer(t.cfg.addr, c)
	if err != nil {
		xEnv.Errorf("%s kafka thread.idx=%d create agent error:%v", t.cfg.name, t.idx, err)
		return err
	}

	t.status = OK
	xEnv.Errorf("%s kafka thread.idx = %d start ok", t.cfg.name, t.idx)

	//开启处理缓存的handler
	t.Handler()

	return nil
}
