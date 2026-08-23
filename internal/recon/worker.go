// Package recon 对账 Worker：小时轻量对账（CH 明细 vs 小时预聚合）+ 每日深度对账
// （CH 明细按 snapshot 重算 vs PG Ledger 流水），差异生成 correction 流水（红冲/补差）。
//
// 对账原则（详细设计文档 §8）：
// - CH meter_events 明细为计量可信源；PG ledger_entries 为财务可信源
// - 定稿账单禁止修改；差异一律走新增 correction 流水
package recon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/metrics"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/store"
)

const reconLock = "meteron:lock:recon"

// Worker 对账 Worker。
type Worker struct {
	db  *pgxpool.Pool
	ch  driver.Conn
	rds *store.Redis
	cfg *config.Config
	log *slog.Logger
}

func NewWorker(db *pgxpool.Pool, ch driver.Conn, rds *store.Redis, cfg *config.Config, log *slog.Logger) *Worker {
	return &Worker{db: db, ch: ch, rds: rds, cfg: cfg, log: log}
}

// Run 启动轻量/深度两个对账循环。
func (w *Worker) Run(ctx context.Context) error {
	go w.lightLoop(ctx)
	w.deepLoop(ctx)
	return nil
}

// lightLoop 小时轻量对账：上一完整小时 CH 明细 vs 小时预聚合（检测 Sink/聚合漂移）。
func (w *Worker) lightLoop(ctx context.Context) {
	interval := w.cfg.Business.ReconLightInterval
	if interval <= 0 {
		interval = time.Hour
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			token, ok, err := w.rds.DistLock(ctx, reconLock+":light", 10*time.Minute)
			if err != nil || !ok {
				continue
			}
			w.runLightAllTenants(ctx)
			w.unlock(ctx, reconLock+":light", token)
		}
	}
}

// deepLoop 每日深度对账：上一自然日 CH 明细重算 vs Ledger 流水，差异补 correction。
func (w *Worker) deepLoop(ctx context.Context) {
	interval := w.cfg.Business.ReconDeepInterval
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			token, ok, err := w.rds.DistLock(ctx, reconLock+":deep", 30*time.Minute)
			if err != nil || !ok {
				continue
			}
			w.runDeepAllTenants(ctx)
			w.unlock(ctx, reconLock+":deep", token)
		}
	}
}

func (w *Worker) runLightAllTenants(ctx context.Context) {
	// 对账 (now-65min) 所在小时：MeterWorker 在小时关闭 +2min 宽限后才落盘
	// hourly 聚合，取更早的小时保证聚合数据已可见，避免误报差异。
	hour := time.Now().UTC().Add(-65 * time.Minute).Truncate(time.Hour)
	tenants, err := w.listTenants(ctx)
	if err != nil {
		w.log.Error("recon light: list tenants failed", "err", err)
		return
	}
	for _, tenantID := range tenants {
		if err := w.runLight(ctx, tenantID, hour); err != nil {
			w.log.Error("recon light failed", "tenant", tenantID, "hour", hour, "err", err)
		}
	}
}

func (w *Worker) runDeepAllTenants(ctx context.Context) {
	now := time.Now().UTC()
	dayStart := now.AddDate(0, 0, -1).Truncate(24 * time.Hour)
	tenants, err := w.listTenants(ctx)
	if err != nil {
		w.log.Error("recon deep: list tenants failed", "err", err)
		return
	}
	for _, tenantID := range tenants {
		if err := w.runDeep(ctx, tenantID, dayStart, dayStart.Add(24*time.Hour)); err != nil {
			w.log.Error("recon deep failed", "tenant", tenantID, "day", dayStart, "err", err)
		}
	}
}

// runLight 轻量对账：明细事件数/token vs 小时预聚合表。
func (w *Worker) runLight(ctx context.Context, tenantID uuid.UUID, hour time.Time) error {
	runID, err := w.createRun(ctx, tenantID, hour, map[string]string{"type": "light"})
	if err != nil {
		return err
	}
	if err := w.markRun(ctx, runID, "running", nil); err != nil {
		return err
	}

	// CH 明细（FINAL 去重后）
	var detailCount, detailTokens uint64
	if err := w.ch.QueryRow(ctx, `
SELECT count(), sum(input_tokens + output_tokens)
FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND event_time >= $2 AND event_time < $3`,
		tenantID.String(), hour, hour.Add(time.Hour)).Scan(&detailCount, &detailTokens); err != nil {
		return w.failRun(ctx, runID, err)
	}
	// 小时预聚合
	var aggCount, aggTokens uint64
	if err := w.ch.QueryRow(ctx, `
SELECT sum(request_count), sum(total_input_tokens + total_output_tokens)
FROM meter_events_hourly
WHERE tenant_id = toUUID($1) AND hour = $2`,
		tenantID.String(), hour).Scan(&aggCount, &aggTokens); err != nil {
		return w.failRun(ctx, runID, err)
	}

	diff := int64(detailCount) - int64(aggCount)
	tokenDiff := int64(detailTokens) - int64(aggTokens)
	if diff != 0 || tokenDiff != 0 {
		metrics.ReconciliationDiffCount.WithLabelValues(tenantID.String()).Add(float64(diff))
		w.log.Warn("recon light: detail vs hourly aggregate mismatch",
			"tenant", tenantID, "hour", hour,
			"detail_count", detailCount, "hourly_count", aggCount,
			"detail_tokens", detailTokens, "hourly_tokens", aggTokens)
	}

	// 轻量对账以 token 总量为核心口径（金额重算属深度对账职责）
	usageTotal := decimal.NewFromInt(int64(detailTokens))
	ledgerTotal := decimal.NewFromInt(int64(aggTokens))
	return w.completeRun(ctx, runID, &RunResult{
		UsageTotal:  usageTotal,
		RatedTotal:  usageTotal,
		LedgerTotal: ledgerTotal,
		DiffAmount:  usageTotal.Sub(ledgerTotal),
		Corrected:   0,
	})
}

// runDeep 深度对账：CH 明细按 snapshot 重算金额 vs Ledger usage_charge+correction 合计。
// 差异（含缺失流水）生成 correction 流水，保持账本不可变。
func (w *Worker) runDeep(ctx context.Context, tenantID uuid.UUID, from, to time.Time) error {
	runID, err := w.createRun(ctx, tenantID, from, map[string]string{"type": "deep", "from": from.Format(time.RFC3339), "to": to.Format(time.RFC3339)})
	if err != nil {
		return err
	}
	if err := w.markRun(ctx, runID, "running", nil); err != nil {
		return err
	}

	// 1. CH 明细：按客户分组读取（FINAL）
	customers, err := meter.DistinctCustomers(ctx, w.ch, tenantID, from, to)
	if err != nil {
		return w.failRun(ctx, runID, err)
	}

	result := &RunResult{}
	ledgerTotal := decimal.Zero
	ratedTotal := decimal.Zero
	usageTokens := decimal.Zero

	for _, customerID := range customers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		events, err := meter.QueryEvents(ctx, w.ch, tenantID, customerID, from, to)
		if err != nil {
			return w.failRun(ctx, runID, err)
		}
		if len(events) == 0 {
			continue
		}

		// 2. 期望金额：按 snapshot 重算（与 BillingRun 同口径：tiered 按产品累计）
		type plan struct {
			eventID uuid.UUID
			cost    decimal.Decimal
		}
		cumByProduct := map[uuid.UUID]uint64{}
		expect := map[uuid.UUID]decimal.Decimal{}
		var plans []plan
		for _, ev := range events {
			usageTokens = usageTokens.Add(decimal.NewFromInt(int64(ev.InputTokens + ev.OutputTokens)))
			snap, perr := pricing.ParseSnapshot([]byte(ev.Snapshot))
			if perr != nil || snap == nil || snap.ProductID == uuid.Nil {
				continue
			}
			cost, newCum := pricing.EventCostCumulative(snap, cumByProduct[snap.ProductID],
				ev.InputTokens, ev.OutputTokens, ev.CachedTokens)
			cumByProduct[snap.ProductID] = newCum
			expect[ev.EventID] = cost
			plans = append(plans, plan{eventID: ev.EventID, cost: cost})
		}
		if len(plans) == 0 {
			continue
		}

		// 3. 实际流水：usage_charge + correction 按 source_event_id 汇总
		ids := make([]string, 0, len(plans))
		for _, p := range plans {
			ids = append(ids, p.eventID.String())
		}
		actual := map[uuid.UUID]decimal.Decimal{}
		rows, err := w.db.Query(ctx, `
SELECT source_event_id::text, SUM(amount)::text
FROM ledger_entries
WHERE tenant_id = $1::uuid AND customer_id = $2::uuid
  AND source_event_id = ANY($3::uuid[])
  AND entry_type IN ('usage_charge','correction')
GROUP BY source_event_id`, tenantID.String(), customerID.String(), ids)
		if err != nil {
			return w.failRun(ctx, runID, err)
		}
		for rows.Next() {
			var idStr, amtStr string
			if err := rows.Scan(&idStr, &amtStr); err != nil {
				rows.Close()
				return w.failRun(ctx, runID, err)
			}
			id, _ := uuid.Parse(idStr)
			amt, _ := decimal.NewFromString(amtStr)
			actual[id] = amt
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return w.failRun(ctx, runID, err)
		}

		// 4. 差异 → correction 流水
		for _, p := range plans {
			exp := expect[p.eventID]
			act := actual[p.eventID]
			ledgerTotal = ledgerTotal.Add(act)
			ratedTotal = ratedTotal.Add(exp)
			if exp.Equal(act) {
				continue
			}
			diff := exp.Sub(act)
			if err := ledger.InsertEntry(ctx, w.db, ledger.Entry{
				TenantID: tenantID, CustomerID: customerID,
				Amount: diff, EntryType: ledger.EntryCorrection,
				SourceEventID: &p.eventID,
				Memo:          fmt.Sprintf("recon correction: expected=%s actual=%s", exp, act),
			}); err != nil {
				return w.failRun(ctx, runID, err)
			}
			ledgerTotal = ledgerTotal.Add(diff) // 补差后账实一致
			result.Corrected++
			w.log.Warn("recon deep: correction inserted",
				"tenant", tenantID, "customer", customerID, "event", p.eventID, "diff", diff)
		}
	}

	result.UsageTotal = usageTokens // 真实用量（input+output token 总数）
	result.RatedTotal = ratedTotal
	result.LedgerTotal = ledgerTotal
	result.DiffAmount = ratedTotal.Sub(ledgerTotal) // 补差后应为 0

	// 与轻量对账一致使用 Add：Set 会覆盖轻量对账累积的差异值（同一指标）
	metrics.ReconciliationDiffCount.WithLabelValues(tenantID.String()).Add(float64(result.Corrected))
	return w.completeRun(ctx, runID, result)
}

// ---- reconciliation_runs 状态机 ----

// RunResult 对账结果。
type RunResult struct {
	UsageTotal  decimal.Decimal `json:"usage_total"`
	RatedTotal  decimal.Decimal `json:"rated_total"`
	LedgerTotal decimal.Decimal `json:"ledger_total"`
	DiffAmount  decimal.Decimal `json:"diff_amount"`
	Corrected   int             `json:"corrected_count"`
}

func (w *Worker) createRun(ctx context.Context, tenantID uuid.UUID, runDate time.Time, scope map[string]string) (uuid.UUID, error) {
	scopeJSON, _ := json.Marshal(scope)
	var idStr string
	if err := w.db.QueryRow(ctx, `
INSERT INTO reconciliation_runs (tenant_id, run_date, scope, status)
VALUES ($1::uuid, $2, $3::jsonb, 'pending') RETURNING id::text`,
		tenantID.String(), runDate, string(scopeJSON)).Scan(&idStr); err != nil {
		return uuid.Nil, err
	}
	id, _ := uuid.Parse(idStr)
	return id, nil
}

func (w *Worker) markRun(ctx context.Context, runID uuid.UUID, status string, result *RunResult) error {
	if status == "running" {
		_, err := w.db.Exec(ctx,
			`UPDATE reconciliation_runs SET status='running', started_at=NOW(), updated_at=NOW() WHERE id=$1::uuid`,
			runID.String())
		return err
	}
	_, err := w.db.Exec(ctx, `
UPDATE reconciliation_runs SET status=$2, completed_at=NOW(), updated_at=NOW(),
  usage_total=$3::numeric, rated_total=$4::numeric, ledger_total=$5::numeric,
  diff_amount=$6::numeric, corrected_count=$7
WHERE id=$1::uuid`,
		runID.String(), status,
		result.UsageTotal.String(), result.RatedTotal.String(),
		result.LedgerTotal.String(), result.DiffAmount.String(), result.Corrected)
	return err
}

func (w *Worker) completeRun(ctx context.Context, runID uuid.UUID, result *RunResult) error {
	return w.markRun(ctx, runID, "completed", result)
}

func (w *Worker) failRun(ctx context.Context, runID uuid.UUID, cause error) error {
	w.log.Error("recon run failed", "run_id", runID, "err", cause)
	if _, err := w.db.Exec(ctx,
		`UPDATE reconciliation_runs SET status='failed', completed_at=NOW(), updated_at=NOW() WHERE id=$1::uuid`,
		runID.String()); err != nil {
		w.log.Error("mark recon run failed status error", "run_id", runID, "err", err)
	}
	return cause
}

func (w *Worker) listTenants(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := w.db.Query(ctx, `SELECT id::text FROM tenants`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if id, err := uuid.Parse(s); err == nil {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

func (w *Worker) unlock(ctx context.Context, key, token string) {
	if err := w.rds.Unlock(ctx, key, token); err != nil {
		w.log.Warn("recon unlock failed (TTL 兜底)", "key", key, "err", err)
	}
}
