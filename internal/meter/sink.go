// Package meter 计量模块：Sink Worker（Kafka→ClickHouse 幂等写入）、
// Meter Worker（小时预聚合）、用量查询。
package meter

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/segmentio/kafka-go"

	"github.com/meteron/meteron/internal/ingest"
)

// SinkWorker 消费 Kafka meter-events，基于 event_id + event_version 幂等写入
// ClickHouse ReplacingMergeTree 明细表（至少一次投递 → 幂等存储去重）。
type SinkWorker struct {
	reader driver.Conn
	rdr    *kafka.Reader
	log    *slog.Logger
}

func NewSinkWorker(ch driver.Conn, brokers []string, topic string, log *slog.Logger) *SinkWorker {
	return &SinkWorker{
		reader: ch,
		rdr: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  "meteron-sink",
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10 << 20,
			MaxWait:  500 * time.Millisecond,
		}),
		log: log,
	}
}

// Run 主循环：Fetch（不自动提交）→ 写 CH 成功后 Commit，保证至少一次投递语义。
func (w *SinkWorker) Run(ctx context.Context) error {
	batch := make([]kafka.Message, 0, 64)
	events := make([]*ingest.MeterEvent, 0, 64)
	flushTicker := time.NewTicker(200 * time.Millisecond)
	defer flushTicker.Stop()

	flush := func() {
		if len(events) == 0 {
			return
		}
		// 失败退避重试：不提交 offset，进程重启后重放（幂等）
		for {
			if err := w.writeEvents(ctx, events); err == nil {
				break
			} else {
				w.log.Error("sink write clickhouse failed, retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
			}
		}
		if err := w.rdr.CommitMessages(ctx, batch...); err != nil {
			w.log.Warn("kafka commit failed (will redeliver, idempotent)", "err", err)
		}
		batch = batch[:0]
		events = events[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return w.rdr.Close()
		case <-flushTicker.C:
			flush()
		default:
		}

		msg, err := w.rdr.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			w.log.Error("kafka fetch failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		var ev ingest.MeterEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			// 毒消息：记录并跳过（提交 offset，避免卡死队列）
			w.log.Error("bad meter event payload, skipped", "err", err, "partition", msg.Partition, "offset", msg.Offset)
			if cerr := w.rdr.CommitMessages(ctx, msg); cerr != nil {
				w.log.Warn("commit poison message failed", "err", cerr)
			}
			continue
		}
		batch = append(batch, msg)
		events = append(events, &ev)
		if len(events) >= 100 {
			flush()
		}
	}
}

func (w *SinkWorker) writeEvents(ctx context.Context, events []*ingest.MeterEvent) error {
	batch, err := w.reader.PrepareBatch(ctx, "INSERT INTO meter_events")
	if err != nil {
		return err
	}
	for _, ev := range events {
		var snapshotJSON string
		if ev.PricingContextSnapshot != nil {
			b, _ := json.Marshal(ev.PricingContextSnapshot)
			snapshotJSON = string(b)
		} else if ev.SnapshotRaw != "" {
			snapshotJSON = ev.SnapshotRaw
		}
		metadata := ev.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		if err := batch.Append(
			ev.TenantID,
			ev.CustomerID,
			ev.EventID.String(),
			ev.EventVersion,
			ev.TransactionID.String(),
			ev.EventType,
			ev.Timestamp.UTC(),
			ev.Provider,
			ev.Model,
			ev.TaskType,
			ev.AgentID,
			ev.InputTokens,
			ev.OutputTokens,
			ev.CachedInputTokens,
			snapshotJSON,
			ev.Currency,
			metadata,
			ev.RawPayload,
			time.Now().UTC(),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}
