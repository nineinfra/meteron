package pricing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Engine 定价引擎：合同 → RateCard → 规则匹配 → 快照。
type Engine struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

func NewEngine(db *pgxpool.Pool, log *slog.Logger) *Engine {
	return &Engine{db: db, log: log}
}

type contractInfo struct {
	ID         uuid.UUID
	RateCardID uuid.UUID
}

// ResolveSnapshot 在请求匹配成功时刻生成计价上下文快照。
// 未匹配到合同/规则时返回零价快照（保持调用链可用，营收侧由配置兜底规则保障）。
func (e *Engine) ResolveSnapshot(ctx context.Context, tenantID, customerID uuid.UUID, dims Dims) (*Snapshot, error) {
	contract, err := e.activeContract(ctx, tenantID, customerID)
	if err != nil {
		return nil, err
	}
	if contract == nil {
		e.log.Warn("no active contract, fallback zero-price snapshot",
			"tenant_id", tenantID, "customer_id", customerID)
		return zeroSnapshot(dims), nil
	}

	cardID, cardVersion, currency, err := e.rateCard(ctx, tenantID, contract.RateCardID)
	if err != nil {
		return nil, err
	}
	if cardID == uuid.Nil {
		e.log.Warn("rate card inactive or expired, fallback zero-price snapshot",
			"tenant_id", tenantID, "customer_id", customerID)
		return zeroSnapshot(dims), nil
	}

	rules, err := e.rules(ctx, cardID)
	if err != nil {
		return nil, err
	}

	rule := pickRule(rules, dims)
	if rule == nil {
		e.log.Warn("no rate rule matched, fallback zero-price snapshot",
			"tenant_id", tenantID, "customer_id", customerID, "dims", fmt.Sprintf("%+v", dims))
		return zeroSnapshot(dims), nil
	}

	return buildSnapshot(rule, cardID, cardVersion, currency, dims), nil
}

func (e *Engine) activeContract(ctx context.Context, tenantID, customerID uuid.UUID) (*contractInfo, error) {
	var idStr, rcStr string
	err := e.db.QueryRow(ctx, `
SELECT c.id::text, c.rate_card_id::text
FROM contracts c
WHERE c.tenant_id = $1::uuid AND c.customer_id = $2::uuid
  AND c.deleted_at IS NULL AND c.is_terminated = false
  AND c.start_time <= NOW() AND (c.end_time IS NULL OR c.end_time > NOW())
ORDER BY c.start_time DESC
LIMIT 1`, tenantID.String(), customerID.String()).Scan(&idStr, &rcStr)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	c := &contractInfo{}
	if c.ID, err = uuid.Parse(idStr); err != nil {
		return nil, err
	}
	if c.RateCardID, err = uuid.Parse(rcStr); err != nil {
		return nil, err
	}
	return c, nil
}

func (e *Engine) rateCard(ctx context.Context, tenantID, cardID uuid.UUID) (id uuid.UUID, version int, currency string, err error) {
	var idStr string
	err = e.db.QueryRow(ctx, `
SELECT id::text, version, currency FROM rate_cards
WHERE id = $1::uuid AND tenant_id = $2::uuid AND deleted_at IS NULL
  AND is_active = true
  AND valid_from <= NOW() AND (valid_to IS NULL OR valid_to > NOW())`,
		cardID.String(), tenantID.String()).Scan(&idStr, &version, &currency)
	if err != nil {
		if err == pgx.ErrNoRows {
			return uuid.Nil, 0, "", nil
		}
		return uuid.Nil, 0, "", err
	}
	if id, err = uuid.Parse(idStr); err != nil {
		return uuid.Nil, 0, "", err
	}
	return id, version, currency, nil
}

func (e *Engine) rules(ctx context.Context, cardID uuid.UUID) ([]*RateRule, error) {
	rows, err := e.db.Query(ctx, `
SELECT id::text, rate_card_id::text, product_id::text,
       match_conditions::text, pricing_type::text, pricing_config::text,
       priority, is_default_rule
FROM rate_card_rules
WHERE rate_card_id = $1::uuid AND deleted_at IS NULL
ORDER BY is_default_rule ASC, priority DESC, created_at ASC`, cardID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*RateRule
	for rows.Next() {
		var idStr, rcStr, pidStr, mcJSON, pcJSON, ptype string
		var priority int
		var isDefault bool
		if err := rows.Scan(&idStr, &rcStr, &pidStr, &mcJSON, &ptype, &pcJSON, &priority, &isDefault); err != nil {
			return nil, err
		}
		r := &RateRule{Priority: priority, IsDefaultRule: isDefault}
		if r.ID, err = uuid.Parse(idStr); err != nil {
			return nil, err
		}
		if r.RateCardID, err = uuid.Parse(rcStr); err != nil {
			return nil, err
		}
		if r.ProductID, err = uuid.Parse(pidStr); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(mcJSON), &r.MatchConditions); err != nil {
			return nil, fmt.Errorf("bad match_conditions for rule %s: %w", idStr, err)
		}
		if err := json.Unmarshal([]byte(pcJSON), &r.PricingConfig); err != nil {
			return nil, fmt.Errorf("bad pricing_config for rule %s: %w", idStr, err)
		}
		r.PricingType = ptype
		out = append(out, r)
	}
	return out, rows.Err()
}

// pickRule 多维度匹配树：所有 match_conditions 键值命中（支持 "*" 通配）。
// rules 已按（非默认优先，priority 降序）排序，取第一条命中。
func pickRule(rules []*RateRule, dims Dims) *RateRule {
	for _, r := range rules {
		if r.IsDefaultRule {
			continue
		}
		if matchConditions(r.MatchConditions, dims) {
			return r
		}
	}
	for _, r := range rules {
		if r.IsDefaultRule && matchConditions(r.MatchConditions, dims) {
			return r
		}
	}
	return nil
}

func matchConditions(conds map[string]string, dims Dims) bool {
	for k, want := range conds {
		var got string
		switch k {
		case "task_type":
			got = dims.TaskType
		case "model":
			got = dims.Model
		case "provider":
			got = dims.Provider
		case "agent_id":
			got = dims.AgentID
		default:
			return false // 未知维度：保守不匹配
		}
		if want != "*" && want != got {
			return false
		}
	}
	return true
}

func buildSnapshot(rule *RateRule, cardID uuid.UUID, cardVersion int, currency string, dims Dims) *Snapshot {
	s := &Snapshot{
		RateCardID:            cardID,
		RateCardVersion:       cardVersion,
		RateRuleID:            rule.ID,
		ProductID:             rule.ProductID,
		Currency:              currency,
		Discount:              "0",
		MatchedConditions:     map[string]string{},
		PricingFormulaVersion: "v1",
	}
	cfg := rule.PricingConfig
	switch rule.PricingType {
	case "tiered":
		s.PricingModel = "tiered"
		s.Tiers = cfg.Tiers
		if len(cfg.Tiers) > 0 {
			s.InputUnitPrice = cfg.Tiers[0].UnitPrice
			s.OutputUnitPrice = cfg.Tiers[0].UnitPrice
			s.CachedInputUnitPrice = cachedPriceOf(cfg, cfg.Tiers[0].CachedInputUnitPrice)
		}
	default:
		s.PricingModel = "flat"
		s.InputUnitPrice = orDefault(cfg.InputUnitPrice, cfg.UnitPrice)
		s.OutputUnitPrice = orDefault(cfg.OutputUnitPrice, cfg.UnitPrice)
		s.CachedInputUnitPrice = cachedPriceOf(cfg, "")
	}
	for k, v := range rule.MatchConditions {
		s.MatchedConditions[k] = v
	}
	return s
}

func cachedPriceOf(cfg PricingConfig, tierCached string) string {
	if tierCached != "" {
		return tierCached
	}
	if cfg.CachedInputUnitPrice != "" {
		return cfg.CachedInputUnitPrice
	}
	return "0"
}

func orDefault(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func zeroSnapshot(dims Dims) *Snapshot {
	return &Snapshot{
		PricingModel:          "flat",
		Currency:              "USD",
		InputUnitPrice:        "0",
		OutputUnitPrice:       "0",
		CachedInputUnitPrice:  "0",
		Discount:              "0",
		MatchedConditions:     map[string]string{"fallback": "no_rule_matched"},
		PricingFormulaVersion: "v1",
	}
}
