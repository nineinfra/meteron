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

// 内存保护阈值。
//
// MeterWorker 采用"落盘后才提交 offset"语义，当前小时的桶必然无法落盘，
// 因此至少会积压一整个小时的消息与 event_id，必须设硬上限，否则高吞吐下必然 OOM。
const (
	// pendingSoftLimit：超出仅告警（提示下游 ClickHouse 可能已故障）。
	pendingSoftLimit = 200_000
	// pendingHardLimit：超出触发背压（暂停消费），宁可 Kafka 堆积也不让进程 OOM。
	// 单条 pendingMsg 仅保留 topic/partition/offset（约 64B），200 万条约 130MB。
	pendingHardLimit = 2_000_000
	// seenIDsLimit：单小时去重集合上限（约 350MB/小时）。
	// 超限后停止记录新 event_id：重投递可能重复累计（由每小时轻量对账检测），
	// 但绝不会因为去重集合无限膨胀而 OOM。
	seenIDsLimit = 5_000_000
)

// MeterWorker 流式预聚合：消费事件按（客户,任务,模型,provider,agent,小时）预聚合，
// 小时关闭后写入 meter_events_hourly（SummingMergeTree）。
// 以 event_id 集合去重，保证 Kafka 重复投递不重复累计。
//
// offset 提交语义：聚合桶在内存中维护，crash 即丢失——因此 offset 延迟到
// 桶成功写入 ClickHouse 后才提交；crash 后 Kafka 从上次提交点重放，内存桶重建，
// 数据不丢（至少一次 + event_id 内存去重）。
type MeterWorker struct {
	ch  driver.Conn
	rdr *kafka.Reader
	log *slog.Logger

	mu          sync.Mutex
	buckets     map[int64]*hourBucket // hour unix → 聚合桶
	inflight    map[int64]struct{}    // 正在写入 ClickHouse 的小时（防止并发重复落盘同一小时）
	pendingMsgs []pendingMsg          // 已聚合未提交 offset 的消息
}

type hourBucket struct {
	hour          time.Time
	agg           map[string]*aggRow
	seenIDs       map[uuid.UUID]struct{}
	dedupOverflow bool // 去重集合已达上限（降级标记，仅用于限频告警）
}

type aggRow struct {
	tenantID, customerID            uuid.UUID
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
		log:      log,
		buckets:  map[int64]*hourBucket{},
		inflight: map[int64]struct{}{},
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
			// 仅保留提交 offset 所需的定位字段：Value/Key/Headers 在聚合完成后即失效，
			// 保留原始载荷会在高吞吐下积压数百 MB~数 GB（raw_payload 单条最大 64KB）。
			msg:  kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset},
			hour: ev.Timestamp.UTC().Truncate(time.Hour).Unix(),
		})
		pending := len(w.pendingMsgs)
		w.mu.Unlock()
		// offset 不在此提交：待 flushClosed 将桶写入 CH 成功后统一提交

		if pending >= pendingHardLimit {
			// 背压：正常情况下已关闭小时的桶会被 flush 并释放 pending，
			// 持续超限说明 ClickHouse 写入失败（offset 无法提交），
			// 此时继续消费只会堆积内存——暂停消费并告警，由 Kafka 侧堆积。
			w.log.Error("meter worker pending buffer at hard limit, pausing consumption "+
				"(ClickHouse write failing? offsets stay uncommitted)",
				"pending", pending, "limit", pendingHardLimit)
			w.flushClosed(time.Now().UTC())
			if w.pendingLen() >= pendingHardLimit {
				select {
				case <-ctx.Done():
					continue
				case <-time.After(5 * time.Second):
				}
				continue
			}
		} else if pending >= pendingSoftLimit && pending%50_000 == 0 {
			w.log.Warn("meter worker pending buffer growing", "pending", pending, "limit", pendingHardLimit)
		}
	}
}

func (w *MeterWorker) pendingLen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pendingMsgs)
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
	if len(b.seenIDs) < seenIDsLimit {
		b.seenIDs[ev.EventID] = struct{}{}
	} else if !b.dedupOverflow {
		// 降级：去重集合已达上限，不再记录新的 event_id。
		// 后果：同小时内 Kafka 重投递可能重复累计（SummingMergeTree 无去重），
		// 由每小时轻量对账（明细 vs 预聚合）检测暴露——优于进程 OOM。
		b.dedupOverflow = true
		w.log.Error("meter hourly dedup set overflow: duplicated redelivery may be double counted "+
			"(recon-detectable); consider lowering retention window or raising seenIDsLimit",
			"hour", hour, "seen", len(b.seenIDs), "limit", seenIDsLimit)
	}

	// 聚合键必须包含租户与客户维度：meter_events_hourly 的排序键为
	// (tenant_id, customer_id, hour, task_type, model, provider)，若仅按
	// task|model|provider|agent 聚合，不同客户（agentID 默认空值，极易撞键）
	// 的用量会被累加到先到的那一行上，造成跨客户串账。
	gk := ev.TenantID.String() + "|" + ev.CustomerID.String() + "|" +
		ev.TaskType + "|" + ev.Model + "|" + ev.Provider + "|" + ev.AgentID
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

// merge 将 other 的聚合结果并入当前桶。
// 场景：桶写入 ClickHouse 期间（锁外执行），同小时的晚到事件会建立新桶；
// 写入失败回写时需要把两个同小时桶合并，否则会重复落盘或丢失数据。
func (b *hourBucket) merge(other *hourBucket) {
	for k, row := range other.agg {
		if cur, ok := b.agg[k]; ok {
			cur.inputTokens += row.inputTokens
			cur.outputTokens += row.outputTokens
			cur.cachedTokens += row.cachedTokens
			cur.count += row.count
			continue
		}
		b.agg[k] = row
	}
	for id := range other.seenIDs {
		if len(b.seenIDs) < seenIDsLimit {
			b.seenIDs[id] = struct{}{}
		}
	}
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
	// cutoff = (now-2min) 所在小时：仅落盘严格早于该小时的桶（小时已关闭 + 2min 宽限）。
	// 不可包含 Equal——否则未关闭的当前小时桶会被提前落盘并删除，
	// 桶内 seenIDs 去重集合随之丢失（同小时内 Kafka 重投递将重复累计）。
	cutoff := now.Add(-2 * time.Minute).Truncate(time.Hour)
	var toFlush []*hourBucket
	for k, b := range w.buckets {
		if b.hour.Before(cutoff) {
			toFlush = append(toFlush, b)
			delete(w.buckets, k)
			w.inflight[k] = struct{}{} // 占位：锁外写入期间该小时不会被重复摘出
		}
	}
	w.mu.Unlock()
	if len(toFlush) == 0 {
		return
	}

	// ClickHouse 写入在锁外执行（单次最长 30s）：持锁写入会完全阻塞 consume，
	// 导致消费停滞、Kafka 分区无进展。摘出的桶由 inflight 保护不会重复落盘。
	flushedHours := make(map[int64]struct{}, len(toFlush))
	var failed []*hourBucket
	for _, b := range toFlush {
		if err := w.writeBucket(b); err != nil {
			w.log.Error("meter hourly write failed", "hour", b.hour, "err", err)
			failed = append(failed, b)
			continue
		}
		flushedHours[b.hour.Unix()] = struct{}{}
		w.log.Info("meter hourly aggregated", "hour", b.hour, "groups", len(b.agg))
	}

	w.mu.Lock()
	for _, b := range failed {
		k := b.hour.Unix()
		delete(w.inflight, k)
		if cur, ok := w.buckets[k]; ok {
			cur.merge(b) // 写入期间已有新桶（晚到事件）：合并，避免重复落盘
		} else {
			w.buckets[k] = b // 写回等待下轮重试；该小时消息不提交 offset
		}
	}
	for k := range flushedHours {
		// 落盘成功：若期间有新桶（晚到事件）则保留，下轮再落盘
		// （SummingMergeTree 同键累加合并，结果正确）。
		delete(w.inflight, k)
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
	// 限时防止 ClickHouse 挂起导致 worker 永久阻塞。
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
