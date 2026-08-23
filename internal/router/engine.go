// Package router 路由策略引擎：RouteRule + BudgetPolicy + FallbackPolicy。
// MVP 实现：任务类型路由 + 预算水位降级（fallback 模型）。
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
}

type policyConfig struct {
	BudgetWatermark float64 `json:"budget_watermark"` // 可用预算水位阈值（0~1），低于则走 fallback
}

// Engine 路由策略引擎。
type Engine struct {
	db *pgxpool.Pool
}

func NewEngine(db *pgxpool.Pool) *Engine {
	return &Engine{db: db}
}

// Resolve 路由决策。
//   - task_type 命中 route_rule：按 priority 最高规则路由（任务驱动，覆盖请求 model）
//   - 未命中：透传请求 model，按 provider.models 反查 provider
//   - availRatio（可用预算/总预算，无预算池时为1）低于规则 watermark 时走 fallback
func (e *Engine) Resolve(ctx context.Context, tenantID uuid.UUID, taskType, requestedModel string, availRatio float64) (*RouteResult, error) {
	var (
		idStr, name, provider, model    string
		fbProvider, fbModel             *string
		policyJSON                      []byte
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

func errNoRouteNoModel(taskType string) error {
	return &noRouteError{task: taskType}
}

type noRouteError struct{ task string }

func (e *noRouteError) Error() string {
	return "no route rule for task_type=" + e.task + " and no model in request"
}
