package store

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// EnsureKafkaTopic 自动创建主题（分区4、副本1），已存在则忽略。
func EnsureKafkaTopic(brokers []string, topic string) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka controller: %w", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	cconn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dial kafka controller: %w", err)
	}
	defer cconn.Close()

	return cconn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     4,
		ReplicationFactor: 1,
	})
}

// NewKafkaWriter 创建按 key（transaction_id）哈希分区的生产者，保证同事务顺序。
func NewKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		Async:                  false,
		WriteTimeout:           10 * time.Second,
	}
}
