package meter

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/meteron/meteron/internal/ingest"
)

func newTestWorker() *MeterWorker {
	return &MeterWorker{
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		buckets:  map[int64]*hourBucket{},
		inflight: map[int64]struct{}{},
	}
}

func testEvent(hour time.Time, tenant, customer uuid.UUID, tokens uint64) *ingest.MeterEvent {
	return &ingest.MeterEvent{
		EventID:     uuid.New(),
		TenantID:    tenant,
		CustomerID:  customer,
		Timestamp:   hour.Add(time.Minute),
		TaskType:    "chat",
		Model:       "gpt-4o",
		Provider:    "openai",
		InputTokens: tokens,
	}
}

// TestConsume_AggregateKeyIncludesTenantAndCustomer 防回归：
// 聚合键必须包含 tenant/customer 维度。meter_events_hourly 排序键为
// (tenant_id, customer_id, hour, task_type, model, provider)，若聚合键缺少这两个维度，
// 不同客户（agentID 常为空，极易撞键）的用量会被累加到先到的那一行上——跨客户串账；
// 同租户内跨客户错配时总量不变，租户级对账无法发现。
func TestConsume_AggregateKeyIncludesTenantAndCustomer(t *testing.T) {
	w := newTestWorker()
	hour := time.Now().UTC().Truncate(time.Hour)
	tenantA, tenantB := uuid.New(), uuid.New()
	custA, custB := uuid.New(), uuid.New()

	w.consume(testEvent(hour, tenantA, custA, 100))
	w.consume(testEvent(hour, tenantB, custB, 7))

	b := w.buckets[hour.Unix()]
	if b == nil {
		t.Fatal("hour bucket not created")
	}
	if len(b.agg) != 2 {
		t.Fatalf("expected 2 aggregate rows (one per customer), got %d", len(b.agg))
	}
	byCustomer := map[uuid.UUID]*aggRow{}
	for _, row := range b.agg {
		if row.tenantID != tenantA && row.tenantID != tenantB {
			t.Fatalf("unexpected tenant in row: %s", row.tenantID)
		}
		byCustomer[row.customerID] = row
	}
	if byCustomer[custA].inputTokens != 100 || byCustomer[custB].inputTokens != 7 {
		t.Fatalf("tokens crossed customers: custA=%d custB=%d",
			byCustomer[custA].inputTokens, byCustomer[custB].inputTokens)
	}
	if byCustomer[custA].tenantID != tenantA || byCustomer[custB].tenantID != tenantB {
		t.Fatalf("tenant/customer mismatch: A=%s/%s B=%s/%s",
			byCustomer[custA].tenantID, byCustomer[custA].customerID,
			byCustomer[custB].tenantID, byCustomer[custB].customerID)
	}
}

// TestConsume_IdempotentByEventID 同一 event_id 重复投递（Kafka 重放）只累计一次。
func TestConsume_IdempotentByEventID(t *testing.T) {
	w := newTestWorker()
	hour := time.Now().UTC().Truncate(time.Hour)
	ev := testEvent(hour, uuid.New(), uuid.New(), 50)

	w.consume(ev)
	w.consume(ev)
	w.consume(ev)

	b := w.buckets[hour.Unix()]
	if len(b.agg) != 1 {
		t.Fatalf("expected 1 aggregate row, got %d", len(b.agg))
	}
	for _, row := range b.agg {
		if row.inputTokens != 50 {
			t.Fatalf("duplicate delivery double counted: input_tokens=%d, want 50", row.inputTokens)
		}
		if row.count != 1 {
			t.Fatalf("request_count=%d, want 1", row.count)
		}
	}
}

// TestBucketMerge 写入失败回写时，同小时的新桶与旧桶合并（不丢、不重复）。
func TestBucketMerge(t *testing.T) {
	hour := time.Now().UTC().Truncate(time.Hour)
	tenant, customer := uuid.New(), uuid.New()

	old := &hourBucket{hour: hour, agg: map[string]*aggRow{}, seenIDs: map[uuid.UUID]struct{}{}}
	ev1 := testEvent(hour, tenant, customer, 10)
	old.seenIDs[ev1.EventID] = struct{}{}
	old.agg["k"] = &aggRow{tenantID: tenant, customerID: customer, inputTokens: 10, count: 1}

	cur := &hourBucket{hour: hour, agg: map[string]*aggRow{}, seenIDs: map[uuid.UUID]struct{}{}}
	ev2 := testEvent(hour, tenant, customer, 5)
	cur.seenIDs[ev2.EventID] = struct{}{}
	cur.agg["k"] = &aggRow{tenantID: tenant, customerID: customer, inputTokens: 5, count: 1}

	cur.merge(old)

	if len(cur.agg) != 1 {
		t.Fatalf("expected merged single row, got %d", len(cur.agg))
	}
	if got := cur.agg["k"].inputTokens; got != 15 {
		t.Fatalf("merged input_tokens=%d, want 15", got)
	}
	if got := cur.agg["k"].count; got != 2 {
		t.Fatalf("merged count=%d, want 2", got)
	}
	if len(cur.seenIDs) != 2 {
		t.Fatalf("seenIDs not merged: %d, want 2", len(cur.seenIDs))
	}
}

func mkMsg(part int, off int64, hour int64) pendingMsg {
	return pendingMsg{
		msg:  kafka.Message{Topic: "meter-events", Partition: part, Offset: off},
		hour: hour,
	}
}

func offsets(msgs []kafka.Message) []int {
	out := make([]int, len(msgs))
	for i, m := range msgs {
		out[i] = int(m.Offset)
	}
	return out
}

// 乱序场景（修复的核心目标）：同分区内晚到的高小时事件先到（未落盘），
// 早小时事件后到（已落盘）。提交高 offset 会越过队首未落盘消息 → crash 后丢数据，
// 因此该分区必须整体不提交，留待高小时桶落盘后一并提交。
func TestCommittablePrefixOutOfOrder(t *testing.T) {
	flushed := map[int64]struct{}{12: {}}
	pending := []pendingMsg{
		mkMsg(0, 0, 13), // 分区0队首：hour13 未落盘 → 阻塞
		mkMsg(0, 1, 12), // hour12 已落盘，但被前驱阻塞 → 不可提交
		mkMsg(0, 2, 12),
		mkMsg(1, 5, 12), // 分区1队首已落盘 → 可提交
		mkMsg(1, 6, 12),
		mkMsg(1, 7, 13), // 分区1遇到未落盘 → 此后全部阻塞
		mkMsg(1, 8, 12),
	}
	toCommit, remaining := committablePrefix(pending, flushed)

	if got := offsets(toCommit); len(got) != 2 || got[0] != 5 || got[1] != 6 {
		t.Fatalf("toCommit offsets = %v, want [5 6]", got)
	}
	if len(remaining) != 5 {
		t.Fatalf("remaining = %d msgs, want 5", len(remaining))
	}
}

// 顺序场景：全部小时已落盘 → 全量提交。
func TestCommittablePrefixAllFlushed(t *testing.T) {
	flushed := map[int64]struct{}{12: {}, 13: {}}
	pending := []pendingMsg{
		mkMsg(0, 0, 12), mkMsg(0, 1, 13),
		mkMsg(1, 0, 12),
	}
	toCommit, remaining := committablePrefix(pending, flushed)
	if len(toCommit) != 3 {
		t.Fatalf("toCommit = %d, want 3", len(toCommit))
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining = %d, want 0", len(remaining))
	}
}

// 队首阻塞解除后（高小时桶落盘），此前被阻塞的低 offset 消息应能补提交。
func TestCommittablePrefixUnblocksAfterFlush(t *testing.T) {
	pending := []pendingMsg{
		mkMsg(0, 0, 13), mkMsg(0, 1, 12), mkMsg(0, 2, 12),
	}
	// 第一轮：仅 hour12 落盘，分区0 队首 hour13 阻塞 → 无可提交
	toCommit, remaining := committablePrefix(pending, map[int64]struct{}{12: {}})
	if len(toCommit) != 0 {
		t.Fatalf("round1 toCommit = %d, want 0", len(toCommit))
	}
	// 第二轮：hour13 也落盘 → 连续前缀全量提交
	toCommit, remaining = committablePrefix(remaining, map[int64]struct{}{12: {}, 13: {}})
	if got := offsets(toCommit); len(got) != 3 {
		t.Fatalf("round2 toCommit offsets = %v, want [0 1 2]", got)
	}
	if len(remaining) != 0 {
		t.Fatalf("round2 remaining = %d, want 0", len(remaining))
	}
}

// 空输入与空落盘集合的边界。
func TestCommittablePrefixEmpty(t *testing.T) {
	toCommit, remaining := committablePrefix(nil, map[int64]struct{}{12: {}})
	if len(toCommit) != 0 || len(remaining) != 0 {
		t.Fatal("empty pending should yield empty results")
	}
	toCommit, remaining = committablePrefix([]pendingMsg{mkMsg(0, 0, 12)}, nil)
	if len(toCommit) != 0 || len(remaining) != 1 {
		t.Fatal("nothing flushed should commit nothing")
	}
}
