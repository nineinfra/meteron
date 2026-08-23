package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/meteron/meteron/internal/metrics"
)

// Producer Kafka 事件生产者。
//
// 故障语义（Failure-Matrix「Kafka down」行）：
// Kafka 不可用时事件进入本地内存缓冲，后台协程周期重放，恢复后不丢事件；
// 进程崩溃可能丢失缓冲事件（MVP 取舍，生产版本应落盘 WAL）。
type Producer struct {
	writer *kafka.Writer
	log    *slog.Logger

	mu      sync.Mutex
	buffer  []bufferedEvent
	maxBuf  int
}

type bufferedEvent struct {
	key   []byte
	value []byte
}

func NewProducer(writer *kafka.Writer, log *slog.Logger) *Producer {
	p := &Producer{writer: writer, log: log, maxBuf: 100000}
	go p.replayLoop()
	return p
}

// Produce 投递计量事件：分区键 = transaction_id，保证同事务顺序；至少一次投递语义。
func (p *Producer) Produce(ctx context.Context, e *MeterEvent) error {
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := []byte(e.TransactionID.String())
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
		Time:  e.Timestamp,
	}); err != nil {
		p.bufferEvent(key, value)
		p.log.Warn("kafka produce failed, buffered for replay", "event_id", e.EventID, "err", err)
	}
	metrics.MeterEventsTotal.WithLabelValues(
		e.TenantID.String(), e.TaskType, e.Model).Inc()
	return nil
}

func (p *Producer) bufferEvent(key, value []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buffer) >= p.maxBuf {
		p.log.Error("event buffer overflow, dropping oldest", "size", len(p.buffer))
		p.buffer = p.buffer[1:]
	}
	p.buffer = append(p.buffer, bufferedEvent{key: key, value: value})
}

func (p *Producer) replayLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		if len(p.buffer) == 0 {
			p.mu.Unlock()
			continue
		}
		// 摘除式取出：重放期间新事件可安全进入 buffer，
		// 溢出截断也不会与待重放集合错位（修复原先前缀假设的竞态）
		pending := p.buffer
		p.buffer = nil
		p.mu.Unlock()

		var remaining []bufferedEvent
		for _, be := range pending {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.writer.WriteMessages(ctx, kafka.Message{Key: be.key, Value: be.value})
			cancel()
			if err != nil {
				remaining = append(remaining, be)
				p.log.Warn("kafka replay failed", "remaining", len(remaining), "err", err)
				break // Kafka 仍不可用，停止本轮
			}
		}
		p.mu.Lock()
		if len(remaining) > 0 {
			// 失败的放回队头，保持投递顺序
			p.buffer = append(remaining, p.buffer...)
		}
		p.mu.Unlock()
		if len(remaining) == 0 {
			p.log.Info("kafka replay drained", "replayed", len(pending))
		}
	}
}

// Close 关闭底层 writer。
func (p *Producer) Close() error {
	return p.writer.Close()
}
