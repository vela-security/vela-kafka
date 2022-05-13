package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/vela-security/vela-public/assert"
	"github.com/vela-security/vela-public/lua"
	"github.com/vela-security/vela-kafka/consumer"
	"github.com/vela-security/vela-kafka/producer"
)

func WithEnv(env assert.Environment) {
	kfk := lua.NewUserKV()

	kfk.Set("none", lua.LNumber(sarama.CompressionNone))
	kfk.Set("gzip", lua.LNumber(sarama.CompressionGZIP))
	kfk.Set("snappy", lua.LNumber(sarama.CompressionSnappy))
	kfk.Set("lz4", lua.LNumber(sarama.CompressionLZ4))
	kfk.Set("zstd", lua.LNumber(sarama.CompressionZSTD))

	kfk.Set("wait_for_all", lua.LNumber(sarama.WaitForAll))
	kfk.Set("wait_for_local", lua.LNumber(sarama.WaitForLocal))
	kfk.Set("no_response", lua.LNumber(sarama.NoResponse))

	producer.WithEnv(env, kfk)
	consumer.WithEnv(env, kfk)
	env.Global("kafka", kfk)
}
