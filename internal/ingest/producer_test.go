package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// ---- 测试替身 ----

type fakeWriter struct {
	mu       sync.Mutex
	written  []kafka.Message
	failNext bool
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		return errors.New("kafka unavailable")
	}
	f.written = append(f.written, msgs...)
	return nil
}

type outboxRecord struct {
	kafkaKey  string
	payload   []byte
	published bool
}

type fakeStore struct {
	mu      sync.Mutex
	rows    map[uuid.UUID]*outboxRecord
	order   []uuid.UUID
	insertE error
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: map[uuid.UUID]*outboxRecord{}}
}

func (f *fakeStore) InsertOutbox(_ context.Context, eventID uuid.UUID, kafkaKey string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertE != nil {
		return f.insertE
	}
	f.rows[eventID] = &outboxRecord{kafkaKey: kafkaKey, payload: payload}
	f.order = append(f.order, eventID)
	return nil
}

func (f *fakeStore) MarkOutboxPublished(_ context.Context, eventID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.rows[eventID]; ok {
		r.published = true
	}
	return nil
}

func (f *fakeStore) LoadOutboxPending(_ context.Context, limit int) ([]OutboxRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []OutboxRow
	for _, id := range f.order {
		if len(out) >= limit {
			break
		}
		if r := f.rows[id]; r != nil && !r.published {
			out = append(out, OutboxRow{EventID: id, KafkaKey: r.kafkaKey, Payload: r.payload})
		}
	}
	return out, nil
}

func (f *fakeStore) CleanupOutbox(_ context.Context, _ time.Duration) error { return nil }

func (f *fakeStore) pendingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.rows {
		if !r.published {
			n++
		}
	}
	return n
}

func (f *fakeStore) publishedCount() int { return len(f.rows) - f.pendingCount() }

func newTestProducer(w KafkaWriter, s OutboxStore) *Producer {
	return &Producer{writer: w, store: s, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func testEvent() *MeterEvent {
	return &MeterEvent{
		EventID:       uuid.New(),
		TransactionID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		TenantID:      uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		EventType:     "llm_request",
		Timestamp:     time.Now().UTC(),
		Provider:      "mock",
		Model:         "mock-chat",
	}
}

// 核心语义 1：事件先落 outbox（持久化），即时投递成功后标记发布。
func TestProducePersistsBeforePublish(t *testing.T) {
	w := &fakeWriter{}
	s := newFakeStore()
	p := newTestProducer(w, s)

	if err := p.Produce(context.Background(), testEvent()); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if len(w.written) != 1 {
		t.Fatalf("kafka writes = %d, want 1", len(w.written))
	}
	if s.pendingCount() != 0 {
		t.Fatalf("pending after success = %d, want 0 (should be marked published)", s.pendingCount())
	}
}

// 核心语义 2（P1 修复点）：Kafka 不可用时事件保留在 outbox，不丢；
// relay 随后重试投递成功。
func TestProduceSurvivesKafkaOutage(t *testing.T) {
	w := &fakeWriter{failNext: true}
	s := newFakeStore()
	p := newTestProducer(w, s)

	ev := testEvent()
	if err := p.Produce(context.Background(), ev); err != nil {
		t.Fatalf("Produce should not fail when kafka down (outbox holds event): %v", err)
	}
	if s.pendingCount() != 1 {
		t.Fatalf("pending after kafka failure = %d, want 1 (event must be retained)", s.pendingCount())
	}
	if len(w.written) != 0 {
		t.Fatalf("kafka writes = %d, want 0", len(w.written))
	}

	// Kafka 恢复，relay 补投
	w.failNext = false
	n, err := p.relayBatch(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("relayBatch = (%d, %v), want (1, nil)", n, err)
	}
	if s.pendingCount() != 0 {
		t.Fatalf("pending after relay = %d, want 0", s.pendingCount())
	}
	if len(w.written) != 1 {
		t.Fatalf("kafka writes after relay = %d, want 1", len(w.written))
	}
	if w.written[0].Key == nil || string(w.written[0].Key) != ev.TransactionID.String() {
		t.Fatalf("relay must preserve kafka key (transaction_id ordering)")
	}
}

// 核心语义 3：outbox 落库失败必须显式报错（PG 不可用时调用方感知，不静默丢事件）。
func TestProduceFailsWhenOutboxUnavailable(t *testing.T) {
	w := &fakeWriter{}
	s := newFakeStore()
	s.insertE = errors.New("pg down")
	p := newTestProducer(w, s)

	if err := p.Produce(context.Background(), testEvent()); err == nil {
		t.Fatalf("Produce must return error when outbox insert fails")
	}
	if len(w.written) != 0 {
		t.Fatalf("kafka writes = %d, want 0 (event never persisted, must not publish)", len(w.written))
	}
}

// relay 批次语义：中途失败即停，已投递进度保留、未投递事件留待下轮。
func TestRelayBatchStopsOnKafkaFailure(t *testing.T) {
	s := newFakeStore()
	p := newTestProducer(&flakyWriter{failAt: 2}, s)

	// 3 个事件：第 1 个投递成功，第 2 个失败，第 3 个应保持 pending
	for i := 0; i < 3; i++ {
		if err := p.store.InsertOutbox(context.Background(), uuid.New(), "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	n, err := p.relayBatch(context.Background())
	if err == nil {
		t.Fatalf("relayBatch should report kafka failure")
	}
	if n != 1 {
		t.Fatalf("published before failure = %d, want 1", n)
	}
	if s.pendingCount() != 2 {
		t.Fatalf("pending after partial failure = %d, want 2", s.pendingCount())
	}
}

type flakyWriter struct {
	failAt int
	calls  int
}

func (f *flakyWriter) WriteMessages(_ context.Context, _ ...kafka.Message) error {
	f.calls++
	if f.calls == f.failAt {
		return errors.New("kafka flaky")
	}
	return nil
}
