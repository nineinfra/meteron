// Package billing 账单跑批：BillingRun 状态机（pending→running→completed/failed）、
// draft 账单生成、Ledger 流水入账、定稿。
//
// 范围边界（详细设计文档 §5.7 / §13）：
// - MVP：draft 草稿账单骨架 + usage_charge 流水；不支持 amending 修正、Commit/True-up
// - 计价永远读取 MeterEvent 内 pricing_context_snapshot，禁止读取当前 RateCard
// - 定稿账单禁止修改；draft 未定稿，跑批重建（幂等）
package billing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/metrics"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/store"
)

// Worker 账单跑批 Worker。
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

const (
	billingLock = "meteron:lock:billing"
	// billingLockTTL 锁 TTL；billingLockRenew 为续期间隔（远小于 TTL，容忍多次续期失败）。
	// 无续期时，跑批耗时超过 TTL 会让锁提前过期，另一实例可并发进入 →
	// 同一周期产生重复 draft 账单（PG 侧已加部分唯一索引兜底，见 006 迁移）。
	billingLockTTL   = 30 * time.Minute
	billingLockRenew = billingLockTTL / 5
	// billingEventWindow 明细拉取窗口：整周期一次性载入会在大客户（百万级事件）上打爆内存。
	billingEventWindow = time.Hour
)

// RunDailyLoop 自动调度：每个 BillingInterval 为全部租户跑批上一自然日（UTC）。
// BillingInterval=0 表示仅手动触发（默认）。
func (w *Worker) RunDailyLoop(ctx context.Context) error {
	interval := w.cfg.Business.BillingInterval
	if interval <= 0 {
		<-ctx.Done()
		return nil
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.RunAllTenantsYesterday(ctx)
		}
	}
}

// RunAllTenantsYesterday 为全部租户跑批上一自然日。
func (w *Worker) RunAllTenantsYesterday(ctx context.Context) {
	token, ok, err := w.rds.DistLock(ctx, billingLock, billingLockTTL)
	if err != nil || !ok {
		return
	}
	defer w.rds.Unlock(context.WithoutCancel(ctx), billingLock, token)

	// 锁续期：全租户跑批可能远超 30min，锁过期会让另一实例并发跑批。
	renewCtx, stopRenew := context.WithCancel(context.WithoutCancel(ctx))
	defer stopRenew()
	go w.renewLock(renewCtx, billingLock, token)

	now := time.Now().UTC()
	dayStart := now.AddDate(0, 0, -1).Truncate(24 * time.Hour)
	tenants, err := w.listTenants(ctx)
	if err != nil {
		w.log.Error("billing: list tenants failed", "err", err)
		return
	}
	for _, tenantID := range tenants {
		if _, err := w.RunBilling(ctx, tenantID, dayStart, dayStart.Add(24*time.Hour)); err != nil {
			w.log.Error("billing run failed", "tenant", tenantID, "period_start", dayStart, "err", err)
		}
	}
}

// renewLock 周期性续期跑批锁；续期失败（锁已被其他实例接管）时告警并停止。
// 仅告警不中断本轮跑批：draft 重建是幂等的，中断反而留下半成品状态；
// 真正的一致性由 bills 表的部分唯一索引与 draft 重建逻辑保证。
func (w *Worker) renewLock(ctx context.Context, key, token string) {
	t := time.NewTicker(billingLockRenew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ok, err := w.rds.RefreshLock(ctx, key, token, billingLockTTL)
			if err != nil {
				w.log.Error("billing: refresh lock failed", "err", err)
				continue
			}
			if !ok {
				w.log.Error("billing: lock lost during run (another instance took over?)")
				return
			}
		}
	}
}

// RunBilling 执行一次 BillingRun（幂等：重复执行同一周期仅重建 draft，不产生重复账单/流水）。
//
// 流程（详细设计文档 附录E.6）：
//  1. INSERT billing_runs(pending) → running
//  2. ClickHouse 读取周期内有用量客户 → 逐客户：明细 FINAL 查询 → snapshot 计价
//     → draft Bill + bill_line_items + LedgerEntry(usage_charge)
//  3. 完成更新 completed（或 failed，含 error_detail）
func (w *Worker) RunBilling(ctx context.Context, tenantID uuid.UUID, periodStart, periodEnd time.Time) (uuid.UUID, error) {
	if !periodEnd.After(periodStart) {
		return uuid.Nil, common.ErrBadRequest.Wrap(errors.New("period_end must be after period_start"))
	}

	// 1. 创建 BillingRun
	var runIDStr string
	if err := w.db.QueryRow(ctx, `
INSERT INTO billing_runs (tenant_id, period_start, period_end, status)
VALUES ($1::uuid, $2, $3, 'pending') RETURNING id::text`,
		tenantID.String(), periodStart, periodEnd).Scan(&runIDStr); err != nil {
		return uuid.Nil, common.ErrInternal.Wrap(err)
	}
	runID, _ := uuid.Parse(runIDStr)
	if err := w.markRun(ctx, runID, "running", nil); err != nil {
		return runID, common.ErrInternal.Wrap(err)
	}
	started := time.Now()

	result, err := w.processPeriod(ctx, tenantID, periodStart, periodEnd)
	status := "completed"
	if err != nil {
		status = "failed"
		w.log.Error("billing run failed", "run_id", runID, "err", err)
	}

	metrics.BillingRunDurationSeconds.WithLabelValues(status).Observe(time.Since(started).Seconds())
	if uerr := w.markRun(ctx, runID, status, result); uerr != nil {
		return runID, common.ErrInternal.Wrap(uerr)
	}
	if err != nil {
		return runID, common.ErrInternal.Wrap(err)
	}
	w.log.Info("billing run completed", "run_id", runID, "period", fmt.Sprintf("[%s,%s)", periodStart, periodEnd),
		"customers", result.TotalCustomers, "bills", result.TotalBills, "errors", result.ErrorCount)
	return runID, nil
}

// RunResult 跑批统计。
type RunResult struct {
	TotalCustomers int            `json:"total_customers"`
	TotalBills     int            `json:"total_bills"`
	ErrorCount     int            `json:"error_count"`
	SkippedFinal   int            `json:"skipped_finalized"`
	Errors         []string       `json:"errors,omitempty"`
}

func (r *RunResult) addError(msg string) {
	r.ErrorCount++
	if len(r.Errors) < 20 {
		r.Errors = append(r.Errors, msg)
	}
}

// processPeriod 周期处理主体。
func (w *Worker) processPeriod(ctx context.Context, tenantID uuid.UUID, periodStart, periodEnd time.Time) (*RunResult, error) {
	result := &RunResult{}

	customers, err := meter.DistinctCustomers(ctx, w.ch, tenantID, periodStart, periodEnd)
	if err != nil {
		return result, fmt.Errorf("query distinct customers: %w", err)
	}
	result.TotalCustomers = len(customers)

	for _, customerID := range customers {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
		if err := w.billCustomer(ctx, tenantID, customerID, periodStart, periodEnd, result); err != nil {
			w.log.Error("bill customer failed", "customer", customerID, "err", err)
			result.addError(fmt.Sprintf("customer %s: %v", customerID, err))
		}
	}
	return result, nil
}

// billCustomer 单客户出账（单事务：bill + line items + ledger entries 同生共死）。
func (w *Worker) billCustomer(ctx context.Context, tenantID, customerID uuid.UUID, periodStart, periodEnd time.Time, result *RunResult) error {
	// 周期合同（时间区间有交集且未终止）
	var contractIDStr string
	err := w.db.QueryRow(ctx, `
SELECT id::text FROM contracts
WHERE tenant_id=$1::uuid AND customer_id=$2::uuid
  AND start_time < $3 AND (end_time IS NULL OR end_time >= $4)
  AND is_terminated = false AND deleted_at IS NULL
ORDER BY start_time DESC LIMIT 1`,
		tenantID.String(), customerID.String(), periodEnd, periodStart).Scan(&contractIDStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no active contract for period")
		}
		return err
	}
	contractID, _ := uuid.Parse(contractIDStr)

	// 定稿账单保护：该周期已 finalized → 跳过（V1 走 amending；MVP 仅告警）。
	// 提前于明细拉取：避免为已定稿周期拉取全量事件（无谓的重查询）。
	var finalizedID *string
	if err := w.db.QueryRow(ctx, `
SELECT id::text FROM bills
WHERE tenant_id=$1::uuid AND customer_id=$2::uuid
  AND period_start=$3 AND period_end=$4 AND status IN ('finalized','amending')
LIMIT 1`,
		tenantID.String(), customerID.String(), periodStart, periodEnd).Scan(&finalizedID); err == nil && finalizedID != nil {
		w.log.Warn("billing skipped: period already finalized (amending is V1, skipped in MVP)",
			"customer", customerID, "period", periodStart, "bill_id", *finalizedID)
		result.SkippedFinal++
		return nil
	} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// PG 事务：重建 draft（删除旧 draft 及其未定稿流水）
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 旧 draft：账单、行项目、关联流水（draft 未定稿未入正式账本，可重建）
	var oldBillID *string
	if err := tx.QueryRow(ctx, `
SELECT id::text FROM bills
WHERE tenant_id=$1::uuid AND customer_id=$2::uuid
  AND period_start=$3 AND period_end=$4 AND status='draft'
LIMIT 1`,
		tenantID.String(), customerID.String(), periodStart, periodEnd).Scan(&oldBillID); err == nil && oldBillID != nil {
		if _, err := tx.Exec(ctx, `DELETE FROM ledger_entries WHERE bill_id=$1::uuid`, *oldBillID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM bill_line_items WHERE bill_id=$1::uuid`, *oldBillID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM bills WHERE id=$1::uuid`, *oldBillID); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// 逐事件按 snapshot 计价（tiered 需按产品累计 token），先算后写
	type lineAgg struct {
		productID uuid.UUID
		snapshot  *pricing.Snapshot
		amount    decimal.Decimal
		tokens    uint64
		taskTypes map[string]struct{}
		models    map[string]struct{}
	}
	lines := map[uuid.UUID]*lineAgg{}
	cumByProduct := map[uuid.UUID]uint64{}
	type entryPlan struct {
		eventID uuid.UUID
		cost    decimal.Decimal
		memo    string
	}
	var entries []entryPlan

	var currency = "USD"
	var totalEvents int

	// 按时间窗口分片拉取明细：整周期一次性载入会在大客户（单周期百万级事件）上打爆内存。
	// tiered 计价依赖事件时间顺序，故按窗口升序分片，累计状态（cumByProduct）跨片保持。
	for win := periodStart; win.Before(periodEnd); {
		winEnd := win.Add(billingEventWindow)
		if winEnd.After(periodEnd) {
			winEnd = periodEnd
		}
		events, err := meter.QueryEvents(ctx, w.ch, tenantID, customerID, win, winEnd)
		if err != nil {
			return fmt.Errorf("query events [%s,%s): %w", win.Format(time.RFC3339), winEnd.Format(time.RFC3339), err)
		}
		for _, ev := range events {
			totalEvents++
			snap, perr := pricing.ParseSnapshot([]byte(ev.Snapshot))
			if perr != nil || snap == nil || snap.ProductID == uuid.Nil {
				result.addError(fmt.Sprintf("event %s: missing/invalid pricing snapshot", ev.EventID))
				continue
			}
			cost, newCum := pricing.EventCostCumulative(snap, cumByProduct[snap.ProductID],
				ev.InputTokens, ev.OutputTokens, ev.CachedTokens)
			cumByProduct[snap.ProductID] = newCum
			if snap.Currency != "" {
				currency = snap.Currency
			}
			entries = append(entries, entryPlan{eventID: ev.EventID, cost: cost, memo: fmt.Sprintf("%s/%s", ev.Provider, ev.Model)})

			la, ok := lines[snap.ProductID]
			if !ok {
				la = &lineAgg{
					productID: snap.ProductID, snapshot: snap,
					taskTypes: map[string]struct{}{}, models: map[string]struct{}{},
				}
				lines[snap.ProductID] = la
			}
			la.amount = la.amount.Add(cost)
			la.tokens += ev.InputTokens + ev.OutputTokens
			la.taskTypes[ev.TaskType] = struct{}{}
			la.models[ev.Model] = struct{}{}
		}
		win = winEnd
	}
	if totalEvents == 0 {
		return nil // 周期内无用量事件
	}
	if len(lines) == 0 {
		return nil // 全部事件无有效快照，不生成空账单
	}

	total := decimal.Zero
	for _, la := range lines {
		total = total.Add(la.amount)
	}

	// 创建 draft 账单
	var billIDStr string
	if err := tx.QueryRow(ctx, `
INSERT INTO bills (tenant_id, customer_id, contract_id, status, period_start, period_end, total_amount, currency)
VALUES ($1::uuid, $2::uuid, $3::uuid, 'draft', $4, $5, $6::numeric, $7)
RETURNING id::text`,
		tenantID.String(), customerID.String(), contractID.String(),
		periodStart, periodEnd, total.String(), currency).Scan(&billIDStr); err != nil {
		return err
	}
	billID, _ := uuid.Parse(billIDStr)

	// Ledger 流水（usage_charge，source_event_id 溯源，携带 bill_id）
	for _, e := range entries {
		evID := e.eventID
		if err := ledger.InsertEntry(ctx, tx, ledger.Entry{
			TenantID: tenantID, CustomerID: customerID,
			Amount: e.cost, EntryType: ledger.EntryUsageCharge,
			SourceEventID: &evID, BillID: &billID,
			Memo: e.memo,
		}); err != nil {
			return fmt.Errorf("insert ledger entry (event %s): %w", e.eventID, err)
		}
	}

	for _, la := range lines {
		snapJSON, _ := json.Marshal(la.snapshot)
		meta, _ := json.Marshal(map[string]interface{}{
			"tokens":     la.tokens,
			"task_types": keys(la.taskTypes),
			"models":     keys(la.models),
		})
		if _, err := tx.Exec(ctx, `
INSERT INTO bill_line_items (bill_id, product_id, amount, quantity, pricing_context_snapshot, metadata)
VALUES ($1::uuid, $2::uuid, $3::numeric, $4::numeric, $5::jsonb, $6::jsonb)`,
			billIDStr, la.productID.String(), la.amount.String(),
			decimal.NewFromInt(int64(la.tokens)).String(), string(snapJSON), string(meta)); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	result.TotalBills++
	return nil
}

// markRun 更新跑批状态与统计。
func (w *Worker) markRun(ctx context.Context, runID uuid.UUID, status string, result *RunResult) error {
	var detail interface{}
	if result != nil {
		b, err := json.Marshal(result)
		if err == nil {
			detail = string(b)
		}
	}
	switch status {
	case "running":
		_, err := w.db.Exec(ctx,
			`UPDATE billing_runs SET status='running', started_at=NOW(), updated_at=NOW() WHERE id=$1::uuid`,
			runID.String())
		return err
	case "completed", "failed":
		_, err := w.db.Exec(ctx, `
UPDATE billing_runs SET status=$2, completed_at=NOW(), updated_at=NOW(),
  total_customers=$3, total_bills=$4, error_count=$5, error_detail=$6::jsonb
WHERE id=$1::uuid`,
			runID.String(), status,
			result.TotalCustomers, result.TotalBills, result.ErrorCount, detail)
		return err
	}
	return nil
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

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
