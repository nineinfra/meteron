package meter

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// UsageSummary 用量汇总行。
type UsageSummary struct {
	TaskType       string `json:"task_type"`
	Model          string `json:"model"`
	Provider       string `json:"provider"`
	InputTokens    uint64 `json:"input_tokens"`
	OutputTokens   uint64 `json:"output_tokens"`
	CachedTokens   uint64 `json:"cached_input_tokens"`
	RequestCount   uint64 `json:"request_count"`
}

// TimeseriesPoint 小时粒度时序点。
type TimeseriesPoint struct {
	Hour         time.Time `json:"hour"`
	InputTokens  uint64    `json:"input_tokens"`
	OutputTokens uint64    `json:"output_tokens"`
	RequestCount uint64    `json:"request_count"`
}

// EventRow 计量明细行（BillingRun / 深度对账使用；必须 FINAL 查询）。
type EventRow struct {
	EventID     uuid.UUID
	EventTime   time.Time
	TaskType    string
	Model       string
	Provider    string
	AgentID     string
	InputTokens  uint64
	OutputTokens uint64
	CachedTokens uint64
	Snapshot    string
	Currency    string
	Metadata    map[string]string
}

// hourlySafeBoundary 返回保证已落盘 meter_events_hourly 的最大小时（含该小时）。
// MeterWorker 在小时关闭 +2min 宽限期后落盘（30s tick），再加 1min 余量：
// (now - 1h - 3min) 所在小时之前的聚合必定可见。
func hourlySafeBoundary(now time.Time) time.Time {
	return now.Add(-time.Hour - 3*time.Minute).UTC().Truncate(time.Hour)
}

// mergeUsageAdd 将一段查询结果累加进 merged（key: task|model|provider）。
func mergeUsageAdd(merged map[string]*UsageSummary, s UsageSummary) {
	k := s.TaskType + "|" + s.Model + "|" + s.Provider
	if m, ok := merged[k]; ok {
		m.InputTokens += s.InputTokens
		m.OutputTokens += s.OutputTokens
		m.CachedTokens += s.CachedTokens
		m.RequestCount += s.RequestCount
		return
	}
	cp := s
	merged[k] = &cp
}

// QueryUsage 用量汇总：已关闭小时走 hourly 预聚合表，未关闭小时（最近约 1h）
// 从明细表 FINAL 实时聚合（SinkWorker 200ms 批次写入），保证前端近实时可见。
func QueryUsage(ctx context.Context, ch driver.Conn, tenantID uuid.UUID, customerID *uuid.UUID, from, to time.Time) ([]UsageSummary, error) {
	// openFrom：未关闭区间起点（hourly 安全边界 + 1h）
	openFrom := hourlySafeBoundary(time.Now().UTC()).Add(time.Hour)
	merged := map[string]*UsageSummary{}

	// 1) 已关闭小时：hourly 预聚合表（规避 FINAL 开销）
	hourlyTo := to
	if openFrom.Before(hourlyTo) {
		hourlyTo = openFrom
	}
	if from.Before(hourlyTo) {
		sql := `
SELECT task_type, model, provider,
       SUM(total_input_tokens), SUM(total_output_tokens), SUM(total_cached_input_tokens), SUM(request_count)
FROM meter_events_hourly
WHERE tenant_id = toUUID($1) AND hour >= $2 AND hour < $3`
		args := []interface{}{tenantID.String(), from, hourlyTo}
		if customerID != nil {
			sql += ` AND customer_id = toUUID($4)`
			args = append(args, customerID.String())
		}
		sql += ` GROUP BY task_type, model, provider`
		rows, err := ch.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var s UsageSummary
			if err := rows.Scan(&s.TaskType, &s.Model, &s.Provider,
				&s.InputTokens, &s.OutputTokens, &s.CachedTokens, &s.RequestCount); err != nil {
				rows.Close()
				return nil, err
			}
			mergeUsageAdd(merged, s)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	// 2) 未关闭小时：明细表 FINAL 实时聚合
	detailFrom := from
	if openFrom.After(detailFrom) {
		detailFrom = openFrom
	}
	if detailFrom.Before(to) {
		sql := `
SELECT task_type, model, provider,
       SUM(input_tokens), SUM(output_tokens), SUM(cached_input_tokens), count()
FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND event_time >= $2 AND event_time < $3`
		args := []interface{}{tenantID.String(), detailFrom, to}
		if customerID != nil {
			sql += ` AND customer_id = toUUID($4)`
			args = append(args, customerID.String())
		}
		sql += ` GROUP BY task_type, model, provider`
		rows, err := ch.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var s UsageSummary
			if err := rows.Scan(&s.TaskType, &s.Model, &s.Provider,
				&s.InputTokens, &s.OutputTokens, &s.CachedTokens, &s.RequestCount); err != nil {
				rows.Close()
				return nil, err
			}
			mergeUsageAdd(merged, s)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	out := make([]UsageSummary, 0, len(merged))
	for _, s := range merged {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestCount > out[j].RequestCount })
	return out, nil
}

// QueryTimeseries 小时粒度时序：已关闭小时走 hourly 表，未关闭小时从明细表聚合，
// 两段按小时合并（前端近实时曲线）。
func QueryTimeseries(ctx context.Context, ch driver.Conn, tenantID uuid.UUID, customerID *uuid.UUID, from, to time.Time) ([]TimeseriesPoint, error) {
	openFrom := hourlySafeBoundary(time.Now().UTC()).Add(time.Hour)
	merged := map[time.Time]*TimeseriesPoint{}

	// 1) 已关闭小时：hourly 预聚合表
	hourlyTo := to
	if openFrom.Before(hourlyTo) {
		hourlyTo = openFrom
	}
	if from.Before(hourlyTo) {
		sql := `
SELECT hour, SUM(total_input_tokens), SUM(total_output_tokens), SUM(request_count)
FROM meter_events_hourly
WHERE tenant_id = toUUID($1) AND hour >= $2 AND hour < $3`
		args := []interface{}{tenantID.String(), from, hourlyTo}
		if customerID != nil {
			sql += ` AND customer_id = toUUID($4)`
			args = append(args, customerID.String())
		}
		sql += ` GROUP BY hour`
		rows, err := ch.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p TimeseriesPoint
			if err := rows.Scan(&p.Hour, &p.InputTokens, &p.OutputTokens, &p.RequestCount); err != nil {
				rows.Close()
				return nil, err
			}
			cp := p
			merged[p.Hour] = &cp
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	// 2) 未关闭小时：明细表按 toStartOfHour 聚合
	detailFrom := from
	if openFrom.After(detailFrom) {
		detailFrom = openFrom
	}
	if detailFrom.Before(to) {
		sql := `
SELECT toStartOfHour(event_time), SUM(input_tokens), SUM(output_tokens), count()
FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND event_time >= $2 AND event_time < $3`
		args := []interface{}{tenantID.String(), detailFrom, to}
		if customerID != nil {
			sql += ` AND customer_id = toUUID($4)`
			args = append(args, customerID.String())
		}
		sql += ` GROUP BY 1`
		rows, err := ch.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p TimeseriesPoint
			if err := rows.Scan(&p.Hour, &p.InputTokens, &p.OutputTokens, &p.RequestCount); err != nil {
				rows.Close()
				return nil, err
			}
			if m, ok := merged[p.Hour]; ok {
				m.InputTokens += p.InputTokens
				m.OutputTokens += p.OutputTokens
				m.RequestCount += p.RequestCount
			} else {
				cp := p
				merged[p.Hour] = &cp
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	hours := make([]time.Time, 0, len(merged))
	for h := range merged {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Before(hours[j]) })
	out := make([]TimeseriesPoint, 0, len(hours))
	for _, h := range hours {
		out = append(out, *merged[h])
	}
	return out, nil
}

// maxEventRows 单次明细查询返回的最大行数（防 OOM）。
//
// 出账与深度对账按客户全量载入内存：大客户单周期可达百万级事件，
// 无上限时进程 RSS 会被结果集直接打爆。超出上限应缩小查询区间
// （billing 已按小时窗口分片拉取，见 billing.Worker.billCustomer）。
const maxEventRows = 1_000_000

// QueryEvents 查询计量明细（FINAL 必带）。
// 结果最多 maxEventRows 行，超出返回错误（调用方需缩小时间区间分片查询）。
func QueryEvents(ctx context.Context, ch driver.Conn, tenantID, customerID uuid.UUID, from, to time.Time) ([]*EventRow, error) {
	// LIMIT maxEventRows+1：多取一行用于识别"超出上限"，避免无界结果集传输
	rows, err := ch.Query(ctx, `
SELECT event_id, event_time, task_type, model, provider, agent_id,
       input_tokens, output_tokens, cached_input_tokens,
       pricing_context_snapshot, currency, metadata
FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND customer_id = toUUID($2) AND event_time >= $3 AND event_time < $4
ORDER BY event_time, event_id
LIMIT $5`,
		tenantID.String(), customerID.String(), from, to, maxEventRows+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventRow
	for rows.Next() {
		if len(out) >= maxEventRows {
			return nil, fmt.Errorf("meter events query returned more than %d rows in [%s, %s): narrow the time range",
				maxEventRows, from.Format(time.RFC3339), to.Format(time.RFC3339))
		}
		var (
			eventIDStr string
			md         map[string]string
		)
		e := &EventRow{}
		if err := rows.Scan(&eventIDStr, &e.EventTime, &e.TaskType, &e.Model, &e.Provider, &e.AgentID,
			&e.InputTokens, &e.OutputTokens, &e.CachedTokens, &e.Snapshot, &e.Currency, &md); err != nil {
			return nil, err
		}
		e.EventID, _ = uuid.Parse(eventIDStr)
		e.Metadata = md
		if e.Metadata == nil {
			e.Metadata = map[string]string{}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DistinctCustomers 查询周期内有用量的客户列表。
func DistinctCustomers(ctx context.Context, ch driver.Conn, tenantID uuid.UUID, from, to time.Time) ([]uuid.UUID, error) {
	rows, err := ch.Query(ctx, `
SELECT DISTINCT toString(customer_id)
FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND event_time >= $2 AND event_time < $3`,
		tenantID.String(), from, to)
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
