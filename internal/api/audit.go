package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/meteron/meteron/internal/common"
)

// AuditFilter 审计日志查询过滤条件（全部可选；时间范围默认近 24h）。
type AuditFilter struct {
	TenantID   string // 必填（服务端注入，调用方不可指定）
	CustomerID string
	TaskType   string
	AgentID    string
	Model      string
	Provider   string
	StatusCode int // 0=不过滤
	From       time.Time
	To         time.Time
	Limit      int  // 默认 100，上限 1000
	Offset     int
}

// auditEventView 审计事件视图（原始请求事件字段，供管理后台排障）。
type auditEventView struct {
	EventID       string            `json:"event_id"`
	TxnID         string            `json:"transaction_id"`
	CustomerID    string            `json:"customer_id"`
	TaskType      string            `json:"task_type"`
	AgentID       string            `json:"agent_id"`
	Provider      string            `json:"provider"`
	Model         string            `json:"model"`
	InputTokens   uint64            `json:"input_tokens"`
	OutputTokens  uint64            `json:"output_tokens"`
	CachedTokens  uint64            `json:"cached_input_tokens"`
	StatusCode    uint16            `json:"status_code"`
	Currency      string            `json:"currency"`
	Metadata      map[string]string `json:"metadata"`
	EventTime     time.Time         `json:"event_time"`
}

// buildAuditQuery 构造 ClickHouse 审计查询（$N 位置参数化，防注入）。
// 返回 SQL 与位置参数；tenant 强制注入（租户隔离不可绕过）。
//
// 绑定约定（与 internal/meter/usage.go 既有用法一致）：clickhouse-go v2 的
// 纯位置参数只绑定 $N / ? 占位符——命名 {name:Type} 占位符需 clickhouse.Named
// 传参，否则参数被静默丢弃、占位符原样发给服务端导致语法错误。
// UUID 列比较必须 toUUID($N) 包裹（字符串参数格式化为带引号字面量）。
func buildAuditQuery(f AuditFilter) (string, []interface{}) {
	var where []string
	args := []interface{}{}

	add := func(condFmt string, v interface{}) {
		where = append(where, fmt.Sprintf(condFmt, len(args)+1))
		args = append(args, v)
	}

	add("tenant_id = toUUID($%d)", f.TenantID)
	if f.CustomerID != "" {
		add("customer_id = toUUID($%d)", f.CustomerID)
	}
	if f.TaskType != "" {
		add("task_type = $%d", f.TaskType)
	}
	if f.AgentID != "" {
		add("agent_id = $%d", f.AgentID)
	}
	if f.Model != "" {
		add("model = $%d", f.Model)
	}
	if f.Provider != "" {
		add("provider = $%d", f.Provider)
	}
	if f.StatusCode > 0 {
		add("status_code = $%d", f.StatusCode)
	}
	if !f.From.IsZero() {
		add("event_time >= $%d", f.From)
	}
	if !f.To.IsZero() {
		add("event_time <= $%d", f.To)
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// limit/offset 为 args 末尾两位（WHERE 条件参数之后）
	sql := fmt.Sprintf(`
SELECT event_id, transaction_id, customer_id, task_type, agent_id, provider, model,
       input_tokens, output_tokens, cached_input_tokens, status_code, currency, metadata, event_time
FROM meter_events FINAL
WHERE %s
ORDER BY event_time DESC
LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "), len(args)+1, len(args)+2)
	args = append(args, limit, f.Offset)
	return sql, args
}

// parseAuditFilter 从查询参数构造过滤条件（tenant 由服务端注入）。
func parseAuditFilter(r *http.Request, tenantID string) (AuditFilter, error) {
	q := r.URL.Query()
	f := AuditFilter{TenantID: tenantID}

	if v := q.Get("customer_id"); v != "" {
		// 提前校验 UUID：非法值交由 ClickHouse toUUID() 解析会变成 500，
		// 用户输入错误应归为 400
		if _, err := uuid.Parse(v); err != nil {
			return f, fmt.Errorf("invalid customer_id %q (uuid required)", v)
		}
		f.CustomerID = v
	}
	f.TaskType = q.Get("task_type")
	f.AgentID = q.Get("agent_id")
	f.Model = q.Get("model")
	f.Provider = q.Get("provider")

	if v := q.Get("status_code"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 65535 {
			return f, fmt.Errorf("invalid status_code %q", v)
		}
		f.StatusCode = n
	}

	// 时间范围：RFC3339；缺省近 24 小时（防止无界扫描）
	now := time.Now().UTC()
	f.To = now
	f.From = now.Add(-24 * time.Hour)
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, fmt.Errorf("invalid from %q", v)
		}
		f.From = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, fmt.Errorf("invalid to %q", v)
		}
		f.To = t
	}

	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			f.Offset = n
		}
	}
	return f, nil
}

// listAuditEvents GET /api/v1/audit/events
// 按客户/任务/Agent/模型/Provider/状态码/时间范围过滤查询原始请求事件（ClickHouse 复用，不新增存储）。
func (s *Server) listAuditEvents(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, "admin")
	if ident == nil {
		return
	}
	f, err := parseAuditFilter(r, ident.TenantID.String())
	if err != nil {
		writeErr(w, errBadRequest(err.Error()))
		return
	}

	sql, args := buildAuditQuery(f)
	rows, err := s.ch.Query(r.Context(), sql, args...)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	defer rows.Close()

	out := []auditEventView{}
	for rows.Next() {
		var v auditEventView
		if err := rows.Scan(
			&v.EventID, &v.TxnID, &v.CustomerID, &v.TaskType, &v.AgentID, &v.Provider, &v.Model,
			&v.InputTokens, &v.OutputTokens, &v.CachedTokens, &v.StatusCode, &v.Currency, &v.Metadata, &v.EventTime,
		); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
		out = append(out, v)
	}
	// rows 迭代中途出错（网络中断/context 取消/解码失败）时 Next() 返回 false
	// 但错误只存在 rows.Err() 中——不检查会返回 200 + 静默截断的部分数据，
	// 审计排障场景下"部分数据"比报错更危险
	if err := rows.Err(); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]interface{}{
		"events": out,
		"total":  len(out),
	})
}
