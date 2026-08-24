// Package router 路由策略引擎：RouteRule + BudgetPolicy + FallbackPolicy + AutoPolicy。
// MVP 实现：任务类型路由 + 预算水位降级（fallback 模型）+ auto 成本分级路由。
package router

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/meteron/meteron/internal/common"
)

// RouteResult 路由决策结果。
type RouteResult struct {
	Provider         string
	Model            string
	FallbackProvider string
	FallbackModel    string
	RuleID           *uuid.UUID
	RuleName         string
	AutoTier         string // auto 模式命中的成本分级（economy/standard/premium），非 auto 为空
}

type policyConfig struct {
	BudgetWatermark float64    `json:"budget_watermark"` // 可用预算水位阈值（0~1），低于则走 fallback
	Auto            *autoPolicy `json:"auto,omitempty"`  // auto 模式成本分级路由策略
}

// autoPolicy policy_config.auto：model=auto 时的成本分级路由策略。
type autoPolicy struct {
	// 分级边界（输入 token 估算）：超过 premium_min_input_tokens 或含图片/工具调用 → premium；
	// 超过 standard_min_input_tokens → standard；否则 economy。
	// 显式 X-Meteron-Quality 头优先于启发式。0 值取默认（8000 / 2000）。
	StandardMinInputTokens uint64 `json:"standard_min_input_tokens"`
	PremiumMinInputTokens  uint64 `json:"premium_min_input_tokens"`
	// tiers 各分级的目标渠道/模型；model 可省略（省略时取该渠道配置序首个模型）。
	Tiers map[string]autoTier `json:"tiers"`
}

// autoTier 单个成本分级的路由目标。
type autoTier struct {
	Provider string `json:"provider"`
	Model    string `json:"model,omitempty"`
}

// 成本分级从低到高（降级方向）。
var tierOrder = []string{"economy", "standard", "premium"}

// RequestProfile auto 路由的请求画像（复杂度评估信号，由网关从请求提取）。
type RequestProfile struct {
	Quality     string // 显式质量要求（X-Meteron-Quality: low/medium/high），空则启发式评估
	InputTokens uint64 // 输入 token 估算
	HasImages   bool   // 多模态（消息含 image_url）
	HasTools    bool   // 工具调用（tools 非空）
}

// TierLookup 渠道分级查找（由 provider.Registry 实现注入）。
type TierLookup interface {
	ProvidersByTier(tier string) []string
	DefaultModelForProvider(name string) (string, bool)
}

// Engine 路由策略引擎。
type Engine struct {
	db     *pgxpool.Pool
	lookup TierLookup
}

func NewEngine(db *pgxpool.Pool) *Engine {
	return &Engine{db: db}
}

// SetLookup 注入渠道分级查找（auto 策略 tier 反查渠道用）。
func (e *Engine) SetLookup(l TierLookup) { e.lookup = l }

// Resolve 路由决策。
//   - task_type 命中 route_rule：按 priority 最高规则路由（任务驱动，覆盖请求 model）
//   - 未命中：透传请求 model，按 provider.models 反查 provider
//   - availRatio（可用预算/总预算，无预算池时为1）低于规则 watermark 时走 fallback
func (e *Engine) Resolve(ctx context.Context, tenantID uuid.UUID, taskType, requestedModel string, availRatio float64) (*RouteResult, error) {
	var (
		idStr, name, provider, model string
		fbProvider, fbModel          *string
		policyJSON                   []byte
	)
	err := e.db.QueryRow(ctx, `
SELECT id::text, name, target_provider, target_model, fallback_provider, fallback_model, policy_config::text
FROM route_rules
WHERE tenant_id = $1::uuid AND task_type = $2 AND is_active = true AND deleted_at IS NULL
ORDER BY priority DESC, created_at ASC
LIMIT 1`, tenantID.String(), taskType).Scan(&idStr, &name, &provider, &model, &fbProvider, &fbModel, &policyJSON)
	if err != nil {
		if err == pgx.ErrNoRows {
			// 无路由规则：透传请求模型（provider 由注册表按 model 反查）
			if requestedModel == "" {
				return nil, common.ErrBadRequest.Wrap(errNoRouteNoModel(taskType))
			}
			return &RouteResult{Model: requestedModel}, nil
		}
		return nil, err
	}

	var policy policyConfig
	_ = json.Unmarshal(policyJSON, &policy)

	res := &RouteResult{Provider: provider, Model: model, RuleName: name}
	if id, err := uuid.Parse(idStr); err == nil {
		res.RuleID = &id
	}
	if fbProvider != nil && fbModel != nil && *fbProvider != "" && *fbModel != "" {
		res.FallbackProvider = *fbProvider
		res.FallbackModel = *fbModel
	}

	// 预算水位降级策略
	if policy.BudgetWatermark > 0 && availRatio < policy.BudgetWatermark && res.FallbackProvider != "" {
		res.Provider, res.Model = res.FallbackProvider, res.FallbackModel
	}
	return res, nil
}

// ResolveAuto auto 模式路由决策（请求 model=auto）：
//  1. 复杂度评估 → 成本分级：显式 X-Meteron-Quality 优先（low/medium/high →
//     economy/standard/premium）；否则启发式（图片/工具调用/长输入 → premium，
//     中等输入 → standard，短输入 → economy）
//  2. 按分级取路由目标：policy.tiers 显式配置的 provider/model；未配置时按
//     provider.cost_tier 反查该分级的第一个渠道（配置序）
//  3. 目标分级无可用渠道时向低成本方向降级（premium→standard→economy）；
//     同时以降级方向下一可用分级作为 fallback（预算水位/渠道健康降级复用）
func (e *Engine) ResolveAuto(ctx context.Context, tenantID uuid.UUID, taskType string, profile RequestProfile, availRatio float64) (*RouteResult, error) {
	rule, policy, err := e.loadRule(ctx, tenantID, taskType)
	if err != nil {
		return nil, err
	}
	var auto autoPolicy
	if policy != nil && policy.Auto != nil {
		auto = *policy.Auto
	}

	tier := auto.evalTier(profile)
	// 预算水位不足时向低成本方向降一级（复用规则 budget_watermark；
	// auto 场景直接选更低成本分级，而非切换到 fallback_provider）
	if policy != nil && policy.BudgetWatermark > 0 && availRatio < policy.BudgetWatermark {
		if i := tierIndex(tier); i > 0 {
			tier = tierOrder[i-1]
		}
	}
	// 逐级尝试：目标分级 → 低成本方向降级
	for i := tierIndex(tier); i >= 0; i-- {
		t := tierOrder[i]
		res, ok := e.autoTargetFor(t, auto, rule)
		if !ok {
			continue
		}
		res.AutoTier = tier
		// fallback：低成本方向的下一可用分级
		for j := i - 1; j >= 0; j-- {
			if fb, ok := e.autoTargetFor(tierOrder[j], auto, rule); ok {
				res.FallbackProvider, res.FallbackModel = fb.Provider, fb.Model
				break
			}
		}
		return res, nil
	}
	return nil, common.ErrNoProvider.Wrap(errNoAutoRoute(taskType, tier))
}

// loadRule 读取命中的路由规则与策略配置（无规则时返回 nil policy，auto 用默认阈值）。
func (e *Engine) loadRule(ctx context.Context, tenantID uuid.UUID, taskType string) (*uuid.UUID, *policyConfig, error) {
	var (
		idStr      string
		policyJSON []byte
	)
	err := e.db.QueryRow(ctx, `
SELECT id::text, policy_config::text
FROM route_rules
WHERE tenant_id = $1::uuid AND task_type = $2 AND is_active = true AND deleted_at IS NULL
ORDER BY priority DESC, created_at ASC
LIMIT 1`, tenantID.String(), taskType).Scan(&idStr, &policyJSON)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var policy policyConfig
	_ = json.Unmarshal(policyJSON, &policy)
	id, perr := uuid.Parse(idStr)
	if perr != nil {
		return nil, &policy, nil
	}
	return &id, &policy, nil
}

// autoTargetFor 返回分级 t 的路由目标；ok=false 表示该分级无可用渠道。
func (e *Engine) autoTargetFor(t string, auto autoPolicy, ruleID *uuid.UUID) (*RouteResult, bool) {
	res := &RouteResult{RuleID: ruleID}
	if tierCfg, ok := auto.Tiers[t]; ok && tierCfg.Provider != "" {
		res.Provider, res.Model = tierCfg.Provider, tierCfg.Model
		if res.Model == "" && e.lookup != nil {
			if m, ok := e.lookup.DefaultModelForProvider(res.Provider); ok {
				res.Model = m
			}
		}
		return res, res.Model != ""
	}
	// 未显式配置：按 provider.cost_tier 反查（配置序第一个）
	if e.lookup == nil {
		return nil, false
	}
	for _, p := range e.lookup.ProvidersByTier(t) {
		m, ok := e.lookup.DefaultModelForProvider(p)
		if !ok {
			continue
		}
		res.Provider, res.Model = p, m
		return res, true
	}
	return nil, false
}

// evalTier 复杂度评估 → 成本分级。
func (a autoPolicy) evalTier(p RequestProfile) string {
	// 显式质量要求优先：低要求走低成本渠道，高要求走高能力渠道
	switch p.Quality {
	case "low":
		return "economy"
	case "medium":
		return "standard"
	case "high":
		return "premium"
	}
	// 启发式：多模态/工具调用对模型能力要求高；长输入需要大上下文能力
	premiumMin := a.PremiumMinInputTokens
	if premiumMin == 0 {
		premiumMin = 8000
	}
	standardMin := a.StandardMinInputTokens
	if standardMin == 0 {
		standardMin = 2000
	}
	if p.HasImages || p.HasTools || p.InputTokens >= premiumMin {
		return "premium"
	}
	if p.InputTokens >= standardMin {
		return "standard"
	}
	return "economy"
}

func tierIndex(t string) int {
	for i, v := range tierOrder {
		if v == t {
			return i
		}
	}
	return -1
}

func errNoRouteNoModel(taskType string) error {
	return &noRouteError{task: taskType}
}

type noRouteError struct{ task string }

func (e *noRouteError) Error() string {
	return "no route rule for task_type=" + e.task + " and no model in request"
}

func errNoAutoRoute(taskType, tier string) error {
	return &noAutoRouteError{task: taskType, tier: tier}
}

type noAutoRouteError struct {
	task string
	tier string
}

func (e *noAutoRouteError) Error() string {
	return "auto route: no provider available for task_type=" + e.task + " tier=" + e.tier
}
