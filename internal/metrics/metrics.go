// Package metrics 定义业务黄金可观测指标（详细设计文档 §11）。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MeterEventsTotal 按租户-任务-模型统计请求事件总量
	MeterEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "meter_events_total",
			Help: "Meter events produced, by tenant/task_type/model",
		},
		[]string{"tenant_id", "task_type", "model"},
	)

	// BudgetReservationConfirmedTotal 成功确认预留计数
	BudgetReservationConfirmedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "budget_reservation_confirmed_total",
			Help: "Successfully confirmed budget reservations",
		},
	)

	// BudgetReservationReleasedTotal 释放预留计数（监控释放占比，识别大量 LLM 超时异常）
	BudgetReservationReleasedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "budget_reservation_released_total",
			Help: "Released budget reservations",
		},
	)

	// BudgetReservationExpiredTotal 过期预留数量
	BudgetReservationExpiredTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "budget_reservation_expired_total",
			Help: "Expired budget reservations (SSE broken / LLM timeout)",
		},
	)

	// BillingRunDurationSeconds 分状态跑批耗时
	BillingRunDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "billing_run_duration_seconds",
			Help:    "Billing run duration by status",
			Buckets: []float64{0.5, 1, 5, 15, 60, 300},
		},
		[]string{"status"},
	)

	// ReconciliationDiffCount 对账发现差异条数（告警触发源）
	ReconciliationDiffCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "reconciliation_diff_count",
			Help: "Differences found by reconciliation runs",
		},
		[]string{"tenant_id"},
	)

	// GatewayLatencySeconds 网关附加开销（不含 LLM 推理）
	GatewayLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "gateway_overhead_seconds",
			Help:    "Gateway added overhead (excluding LLM inference)",
			Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5},
		},
	)

	// BudgetCacheDriftMicros Redis-PG 预算缓存漂移（微单位）
	BudgetCacheDriftMicros = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "budget_cache_drift_micros",
			Help: "Redis vs PG budget cache drift (micro units)",
		},
		[]string{"pool_id"},
	)
)
