// Package pricing 定价引擎：多维度匹配树、pricing_context_snapshot 快照生成、
// flat/tiered 计价。计费永远读取 MeterEvent 内快照，禁止读取当前最新 RateCard 版本。
package pricing

import (
	"encoding/json"

	"github.com/google/uuid"
)

// PricingConfig rate_card_rules.pricing_config 的 JSON 结构。
//
// flat 示例: {"unit_price":"0.000003","cached_input_unit_price":"0.0000003"}
// unit_price 缺省时必须提供 input_unit_price / output_unit_price。
//
// tiered 示例: {"tiers":[{"upper":10000,"unit_price":"0.000003"},{"upper":null,"unit_price":"0.000002"}]}
type PricingConfig struct {
	UnitPrice            string `json:"unit_price,omitempty"`
	InputUnitPrice       string `json:"input_unit_price,omitempty"`
	OutputUnitPrice      string `json:"output_unit_price,omitempty"`
	CachedInputUnitPrice string `json:"cached_input_unit_price,omitempty"`
	Tiers                []Tier `json:"tiers,omitempty"`
}

// Tier 阶梯定价层：upper 为累计用量上界（null=无穷）。
type Tier struct {
	Upper                 *int64 `json:"upper"`
	UnitPrice             string `json:"unit_price"`
	CachedInputUnitPrice  string `json:"cached_input_unit_price,omitempty"`
}

// Snapshot 计价上下文快照：请求匹配成功时刻固化进 MeterEvent。
// 字段名与详细设计文档 §4 MeterEvent JSON Schema 保持一致。
type Snapshot struct {
	RateCardID             uuid.UUID          `json:"rate_card_id"`
	RateCardVersion        int                `json:"rate_card_version"`
	RateRuleID             uuid.UUID          `json:"rate_rule_id"`
	ProductID              uuid.UUID          `json:"product_id"`
	PricingModel           string             `json:"pricing_model"` // flat | tiered
	Currency               string             `json:"currency"`
	InputUnitPrice         string             `json:"input_unit_price"`
	OutputUnitPrice        string `json:"output_unit_price"`
	CachedInputUnitPrice   string             `json:"cached_input_unit_price"`
	Tiers                  []Tier             `json:"tiers,omitempty"`
	Discount               string             `json:"discount"`
	ContractOverride       bool               `json:"contract_override"`
	MatchedConditions      map[string]string  `json:"matched_conditions"`
	PricingFormulaVersion  string             `json:"pricing_formula_version"`
}

// MarshalJSON 序列化为字符串（ClickHouse meter_events.pricing_context_snapshot 为 String 列）。
func (s *Snapshot) MarshalJSON() ([]byte, error) {
	type alias Snapshot
	return json.Marshal((*alias)(s))
}

// ParseSnapshot 从 JSON 字符串还原快照。
func ParseSnapshot(data []byte) (*Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// RateRule rate_card_rules 行。
type RateRule struct {
	ID              uuid.UUID
	RateCardID      uuid.UUID
	ProductID       uuid.UUID
	MatchConditions map[string]string
	PricingType     string // flat | tiered
	PricingConfig   PricingConfig
	Priority        int
	IsDefaultRule   bool
}

// Dims 计价维度。
type Dims struct {
	TaskType string
	Model    string
	Provider string
	AgentID  string
}
