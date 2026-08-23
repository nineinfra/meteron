package meter

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/meteron/meteron/internal/ingest"
)

// MeterWorker 流式预聚合：消费事件按（客户,任务,模型,provider,agent,小时）预聚合，
// 小时关闭后写入 meter_events_hourly（SummingMergeTree）。
// 以 event_id 集合去重，保证 Kafka 重复投递不重复累计。
//
// offset 提交语义：聚合桶在内存中维护，crash 即丢失——因此 offset 延迟到
// 桶成功写入 ClickHouse 后才提交；crash 后 Kafka 从上次提交点重放，内存桶重建，
// 数据不丢（至少一次 + event_id 内存去重）。
type MeterWorker struct {
	ch     driver.Conn
	rdr    *kafka.Reader
	log    *slog.Logger

	mu          sync.Mutex
	buckets     map[int64]*hourBucket // hour unix → 聚合桶
	pendingMsgs []pendingMsg          // 已聚合未提交 offset 的消息
}

type hourBucket struct {
	hour    time.Time
	agg     map[string]*aggRow
	seenIDs map[uuid.UUID]struct{}
}

type aggRow struct {
	tenantID, customerID     uuid.UUID
	taskType, model, provider, agentID string
	inputTokens, outputTokens, cachedTokens, count uint64
}

func NewMeterWorker(ch driver.Conn, brokers []string, topic string, log *slog.Logger) *MeterWorker {
	return &MeterWorker{
		ch: ch,
		rdr: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  "meteron-meter",
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10 << 20,
			MaxWait:  500 * time.Millisecond,
		}),
		log:     log,
		buckets: map[int64]*hourBucket{},
	}
}

func (w *MeterWorker) Run(ctx context.Context) error {
	flushTicker := time.NewTicker(30 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.flushClosed(time.Now().UTC())
			return w.rdr.Close()
		case <-flushTicker.C:
			w.flushClosed(time.Now().UTC())
		default:
		}

		msg, err := w.rdr.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			w.log.Error("meter worker fetch failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		var ev ingest.MeterEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			w.log.Error("meter worker bad payload, skipped", "err", err)
			_ = w.rdr.CommitMessages(ctx, msg)
			continue
		}
		w.consume(&ev)
		w.mu.Lock()
		w.pendingMsgs = append(w.pendingMsgs, pendingMsg{
			msg:  msg,
			hour: ev.Timestamp.UTC().Truncate(time.Hour).Unix(),
		})
		w.mu.Unlock()
		// offset 不在此提交：待 flushClosed 将桶写入 CH 成功后统一提交
	}
}

func (w *MeterWorker) consume(ev *ingest.MeterEvent) {
	hour := ev.Timestamp.UTC().Truncate(time.Hour)
	key := hour.Unix()

	w.mu.Lock()
	defer w.mu.Unlock()
	b, ok := w.buckets[key]
	if !ok {
		b = &hourBucket{hour: hour, agg: map[string]*aggRow{}, seenIDs: map[uuid.UUID]struct{}{}}
		w.buckets[key] = b
	}
	if _, dup := b.seenIDs[ev.EventID]; dup {
		return // 幂等：重复投递不重复累计
	}
	b.seenIDs[ev.EventID] = struct{}{}

	gk := ev.TaskType + "|" + ev.Model + "|" + ev.Provider + "|" + ev.AgentID
	row, ok := b.agg[gk]
	if !ok {
		row = &aggRow{
			tenantID: ev.TenantID, customerID: ev.CustomerID,
			taskType: ev.TaskType, model: ev.Model, provider: ev.Provider, agentID: ev.AgentID,
		}
		b.agg[gk] = row
	}
	row.inputTokens += ev.InputTokens
	row.outputTokens += ev.OutputTokens
	row.cachedTokens += ev.CachedInputTokens
	row.count++
}

// pendingMsg 已聚合但 offset 未提交的消息（携带所属小时，用于桶落盘后按小时提交）。
type pendingMsg struct {
	msg  kafka.Message
	hour int64
}

// flushClosed 落盘已关闭小时（当前小时 - 2min 宽限期之前的桶）。
// 桶写入成功后，才提交该小时对应消息的 offset（crash 后从上次提交点重放，不丢数据）。
func (w *MeterWorker) flushClosed(now time.Time) {
	w.mu.Lock()
	cutoff := now.Add(-2 * time.Minute).Truncate(time.Hour)
	var toFlush []*hourBucket
	for k, b := range w.buckets {
		if b.hour.Before(cutoff) || b.hour.Equal(cutoff) {
			toFlush = append(toFlush, b)
			delete(w.buckets, k)
		}
	}
	if len(toFlush) == 0 {
		w.mu.Unlock()
		return
	}
	flushedHours := map[int64]struct{}{}
	for _, b := range toFlush {
		if err := w.writeBucket(b); err != nil {
			// 写回桶，等待下轮重试；该小时消息不提交 offset
			w.log.Error("meter hourly write failed", "hour", b.hour, "err", err)
			w.buckets[b.hour.Unix()] = b
		} else {
			flushedHours[b.hour.Unix()] = struct{}{}
			w.log.Info("meter hourly aggregated", "hour", b.hour, "groups", len(b.agg))
		}
	}
	if len(flushedHours) == 0 {
		w.mu.Unlock()
		return
	}
	// 提交已落盘小时的消息 offset；未关闭小时的消息继续留在 pending。
	// 必须按分区提交“连续已落盘前缀”，防止乱序事件下越过未落盘消息（详见 committablePrefix）。
	toCommit, remaining := committablePrefix(w.pendingMsgs, flushedHours)
	w.pendingMsgs = remaining
	w.mu.Unlock()

	if len(toCommit) > 0 {
		if err := w.rdr.CommitMessages(context.Background(), toCommit...); err != nil {
			// 提交失败：crash 后重放会重复累计该小时（SummingMergeTree 无去重），
			// 由每小时轻量对账（明细 vs 预聚合）检测暴露
			w.log.Warn("meter worker commit failed (may reduplicate on replay, recon-detectable)",
				"count", len(toCommit), "err", err)
		}
	}
}

func (w *MeterWorker) writeBucket(b *hourBucket) error {
	if len(b.agg) == 0 {
		return nil
	}
	// 限时防止 ClickHouse 挂起导致 worker 永久阻塞：
	// flushClosed 持锁调用本函数，若无限等待将无法响应 ctx 取消（优雅退出）。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	batch, err := w.ch.PrepareBatch(ctx, "INSERT INTO meter_events_hourly")
	if err != nil {
		return err
	}
	for _, row := range b.agg {
		if err := batch.Append(
			row.tenantID, row.customerID, row.taskType, row.model, row.provider, row.agentID,
			b.hour, row.inputTokens, row.outputTokens, row.cachedTokens, row.count,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// committablePrefix 从 pending 消息中提取可安全提交的“连续已落盘前缀”。
//
// Kafka 同一分区内 offset 单调递增，committed offset 会被推至所提交消息的最大 offset：
// 若晚到事件（高小时桶，尚未落盘）先到、早小时事件（已落盘）后到，直接提交后者
// 会越过前者尚未提交的低 offset——crash 重启后消费从 committed offset 开始，
// 被越过的消息永久丢失。因此每个分区仅提交自队首起、所属小时全部落盘的连续前缀，
// 遇到第一个未落盘消息即停止该分区的提交（该分区其余消息留待下轮）。
func committablePrefix(pending []pendingMsg, flushedHours map[int64]struct{}) (toCommit []kafka.Message, remaining []pendingMsg) {
	blocked := map[int]bool{}
	for _, pm := range pending {
		if !blocked[pm.msg.Partition] {
			if _, ok := flushedHours[pm.hour]; ok {
				toCommit = append(toCommit, pm.msg)
				continue
			}
			blocked[pm.msg.Partition] = true // 队首即未落盘：该分区本轮不提交
		}
		remaining = append(remaining, pm)
	}
	return toCommit, remaining
}
