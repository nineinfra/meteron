package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/meteron/meteron/internal/metrics"
)

const (
	relayBatchSize  = 500             // relay 单批最大事件数
	relayInterval   = 5 * time.Second // relay 扫描周期
	outboxRetention = 7 * 24 * time.Hour
)

// KafkaWriter Kafka 写入抽象（便于单测替换 *kafka.Writer）。
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// OutboxRow 待投递的 outbox 事件。
type OutboxRow struct {
	EventID  uuid.UUID
	KafkaKey string
	Payload  []byte
}

// OutboxStore outbox 持久化访问（生产实现为 pgOutboxStore，单测可伪造）。
type OutboxStore interface {
	InsertOutbox(ctx context.Context, eventID uuid.UUID, kafkaKey string, payload []byte) error
	MarkOutboxPublished(ctx context.Context, eventID uuid.UUID) error
	LoadOutboxPending(ctx context.Context, limit int) ([]OutboxRow, error)
	CleanupOutbox(ctx context.Context, retain time.Duration) error
}

// pgOutboxStore 基于 pgxpool 的 outbox 存储实现。
type pgOutboxStore struct{ pool *pgxpool.Pool }

func NewPGOutboxStore(pool *pgxpool.Pool) OutboxStore {
	return &pgOutboxStore{pool: pool}
}

func (s *pgOutboxStore) InsertOutbox(ctx context.Context, eventID uuid.UUID, kafkaKey string, payload []byte) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO meter_outbox (event_id, kafka_key, payload) VALUES ($1::uuid, $2, $3)`,
		eventID.String(), kafkaKey, payload)
	return err
}

func (s *pgOutboxStore) MarkOutboxPublished(ctx context.Context, eventID uuid.UUID) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE meter_outbox SET published_at=NOW() WHERE event_id=$1::uuid`, eventID.String())
	return err
}

func (s *pgOutboxStore) LoadOutboxPending(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, err := s.pool.Query(ctx, `
SELECT event_id::text, kafka_key, payload FROM meter_outbox
WHERE published_at IS NULL ORDER BY created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var idStr string
		var r OutboxRow
		if err := rows.Scan(&idStr, &r.KafkaKey, &r.Payload); err != nil {
			return nil, err
		}
		if r.EventID, err = uuid.Parse(idStr); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *pgOutboxStore) CleanupOutbox(ctx context.Context, retain time.Duration) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM meter_outbox WHERE published_at IS NOT NULL AND published_at < NOW() - $1::interval`,
		retain.String())
	return err
}

// Producer Kafka 事件生产者（PG outbox 模式）。
//
// 故障语义（P1 修复）：
//   - Produce 先将事件落 PG outbox（持久化），再尽力即时投递 Kafka；
//   - 即时投递失败不影响正确性——relay 周期扫描未发布事件重试；
//   - 进程崩溃不丢事件（outbox 已落盘），重启后 relay 继续投递；
//   - 至少一次语义：即时投递成功但标记失败/多副本并发 relay 可能重复投递，
//     下游 ClickHouse 按 event_id 幂等去重（ReplacingMergeTree），语义安全。
type Producer struct {
	writer KafkaWriter
	store  OutboxStore
	log    *slog.Logger
	stop   chan struct{}
	done   chan struct{}
}

func NewProducer(writer KafkaWriter, store OutboxStore, log *slog.Logger) *Producer {
	p := &Producer{
		writer: writer, store: store, log: log,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go p.relayLoop()
	return p
}

// Produce 投递计量事件：分区键 = transaction_id，保证同事务顺序。
// outbox 落库失败时返回错误（调用方感知），事件不进入 Kafka 也不静默丢弃。
func (p *Producer) Produce(ctx context.Context, e *MeterEvent) error {
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := e.TransactionID.String()
	if err := p.store.InsertOutbox(ctx, e.EventID, key, value); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	// 尽力即时投递：失败留给 relay，成功则立即标记（缩短端到端延迟）
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  e.Timestamp,
	}); err != nil {
		p.log.Warn("kafka produce failed, left in outbox for relay", "event_id", e.EventID, "err", err)
	} else if err := p.store.MarkOutboxPublished(ctx, e.EventID); err != nil {
		// 已投递但标记失败：relay 会重投一次，下游按 event_id 去重
		p.log.Warn("mark outbox published failed (relay will re-publish, deduped downstream)",
			"event_id", e.EventID, "err", err)
	}
	metrics.MeterEventsTotal.WithLabelValues(
		e.TenantID.String(), e.TaskType, e.Model).Inc()
	return nil
}

// relayBatch 取一批未发布事件投递 Kafka。
// Kafka 失败即停本轮（Kafka 不可用时快速让出，避免逐条超时）；
// 已投递的事件立即标记，失败前的进度不会回退。
func (p *Producer) relayBatch(ctx context.Context) (int, error) {
	batch, err := p.store.LoadOutboxPending(ctx, relayBatchSize)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, r := range batch {
		if err := p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(r.KafkaKey), Value: r.Payload}); err != nil {
			return published, err
		}
		if err := p.store.MarkOutboxPublished(ctx, r.EventID); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

func (p *Producer) relayLoop() {
	defer close(p.done)
	ticker := time.NewTicker(relayInterval)
	defer ticker.Stop()
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			n, err := p.relayBatch(ctx)
			cancel()
			if err != nil {
				p.log.Warn("outbox relay failed", "published", n, "err", err)
			} else if n > 0 {
				p.log.Info("outbox relay drained", "published", n)
			}
		case <-cleanupTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := p.store.CleanupOutbox(ctx, outboxRetention); err != nil {
				p.log.Warn("outbox cleanup failed", "err", err)
			}
			cancel()
		}
	}
}

// Close 停止 relay 循环（writer 归属创建方，由其自行关闭）。
func (p *Producer) Close() {
	close(p.stop)
	<-p.done
}
