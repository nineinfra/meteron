// Package ingest 标准化计量事件模型与 Kafka 投递。
// event_id：单条计量事件全局唯一（Sink Worker 幂等去重键）；
// transaction_id：单次 LLM 业务请求 ID（一条业务请求可产出多条 event，Kafka 分区键）；
// X-Idempotency-Key：外部调用方幂等键（网关侧映射为 transaction_id）。
package ingest

import (
	"time"

	"github.com/google/uuid"

	"github.com/meteron/meteron/internal/pricing"
)

// MeterEvent 标准化计量事件（含完整计价上下文快照）。
type MeterEvent struct {
	EventID                 uuid.UUID          `json:"event_id"`
	TransactionID           uuid.UUID          `json:"transaction_id"`
	EventVersion            uint64             `json:"event_version"`
	TenantID                uuid.UUID          `json:"tenant_id"`
	CustomerID              uuid.UUID          `json:"customer_id"`
	EventType               string             `json:"event_type"`
	Timestamp               time.Time          `json:"timestamp"`
	Provider                string             `json:"provider"`
	Model                   string             `json:"model"`
	TaskType                string             `json:"task_type"`
	AgentID                 string             `json:"agent_id"`
	InputTokens             uint64             `json:"input_tokens"`
	OutputTokens            uint64            `json:"output_tokens"`
	CachedInputTokens       uint64             `json:"cached_input_tokens"`
	PricingContextSnapshot  *pricing.Snapshot  `json:"pricing_context_snapshot,omitempty"`
	SnapshotRaw             string             `json:"-"`
	Currency                string             `json:"currency"`
	Metadata                map[string]string  `json:"metadata"`
	RawPayload              string             `json:"raw_payload"`
}
