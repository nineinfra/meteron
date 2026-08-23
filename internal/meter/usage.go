package meter

import (
	"context"
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

// QueryUsage 用量汇总（报表优先使用 hourly 预聚合表，规避 FINAL 开销）。
func QueryUsage(ctx context.Context, ch driver.Conn, tenantID uuid.UUID, customerID *uuid.UUID, from, to time.Time) ([]UsageSummary, error) {
	sql := `
SELECT task_type, model, provider,
       SUM(total_input_tokens), SUM(total_output_tokens), SUM(total_cached_input_tokens), SUM(request_count)
FROM meter_events_hourly
WHERE tenant_id = toUUID($1) AND hour >= $2 AND hour < $3`
	args := []interface{}{tenantID.String(), from, to}
	if customerID != nil {
		sql += ` AND customer_id = toUUID($4)`
		args = append(args, customerID.String())
	}
	sql += ` GROUP BY task_type, model, provider ORDER BY request_count DESC`

	rows, err := ch.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsageSummary
	for rows.Next() {
		var s UsageSummary
		if err := rows.Scan(&s.TaskType, &s.Model, &s.Provider,
			&s.InputTokens, &s.OutputTokens, &s.CachedTokens, &s.RequestCount); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// QueryTimeseries 小时粒度时序。
func QueryTimeseries(ctx context.Context, ch driver.Conn, tenantID uuid.UUID, customerID *uuid.UUID, from, to time.Time) ([]TimeseriesPoint, error) {
	sql := `
SELECT hour, SUM(total_input_tokens), SUM(total_output_tokens), SUM(request_count)
FROM meter_events_hourly
WHERE tenant_id = toUUID($1) AND hour >= $2 AND hour < $3`
	args := []interface{}{tenantID.String(), from, to}
	if customerID != nil {
		sql += ` AND customer_id = toUUID($4)`
		args = append(args, customerID.String())
	}
	sql += ` GROUP BY hour ORDER BY hour`

	rows, err := ch.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TimeseriesPoint
	for rows.Next() {
		var p TimeseriesPoint
		if err := rows.Scan(&p.Hour, &p.InputTokens, &p.OutputTokens, &p.RequestCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// QueryEvents 查询计量明细（FINAL 必带）。
func QueryEvents(ctx context.Context, ch driver.Conn, tenantID, customerID uuid.UUID, from, to time.Time) ([]*EventRow, error) {
	rows, err := ch.Query(ctx, `
SELECT event_id, event_time, task_type, model, provider, agent_id,
       input_tokens, output_tokens, cached_input_tokens,
       pricing_context_snapshot, currency, metadata
FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND customer_id = toUUID($2) AND event_time >= $3 AND event_time < $4
ORDER BY event_time, event_id`,
		tenantID.String(), customerID.String(), from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventRow
	for rows.Next() {
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
