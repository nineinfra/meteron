package api

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// buildAuditQuery：租户强制注入 + 各维度过滤 + 参数化防注入。

func TestBuildAuditQueryTenantAlwaysInjected(t *testing.T) {
	sql, args := buildAuditQuery(AuditFilter{TenantID: "11111111-1111-1111-1111-111111111111"})
	if !strings.Contains(sql, "tenant_id = {tenant:UUID}") {
		t.Fatalf("sql must enforce tenant filter: %s", sql)
	}
	if len(args) == 0 || args[0] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("first arg must be tenant id: %v", args)
	}
	// 只有 tenant 时不应有其他 WHERE 条件（AND 数量=0）
	if got := strings.Count(sql, " AND "); got != 0 {
		t.Fatalf("unexpected extra conditions: %s", sql)
	}
}

func TestBuildAuditQueryAllFilters(t *testing.T) {
	from := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	sql, args := buildAuditQuery(AuditFilter{
		TenantID:   "11111111-1111-1111-1111-111111111111",
		CustomerID: "22222222-2222-2222-2222-222222222222",
		TaskType:   "chat",
		AgentID:    "agent-1",
		Model:      "gpt-4",
		Provider:   "openai",
		StatusCode: 429,
		From:       from,
		To:         to,
		Limit:      50,
		Offset:     10,
	})
	for _, cond := range []string{
		"customer_id = {customer:UUID}",
		"task_type = {task_type:String}",
		"agent_id = {agent_id:String}",
		"model = {model:String}",
		"provider = {provider:String}",
		"status_code = {status_code:UInt16}",
		"event_time >= {from:DateTime64(3)}",
		"event_time <= {to:DateTime64(3)}",
	} {
		if !strings.Contains(sql, cond) {
			t.Errorf("missing condition %q in: %s", cond, sql)
		}
	}
	// 8 过滤条件 + tenant = 9 个条件 => 8 个 AND
	if got := strings.Count(sql, " AND "); got != 8 {
		t.Errorf("AND count = %d, want 8", got)
	}
	// args = tenant + 8 过滤值 + limit + offset
	if len(args) != 11 {
		t.Fatalf("args count = %d, want 11: %v", len(args), args)
	}
	if !strings.Contains(sql, "LIMIT {limit:UInt32} OFFSET {offset:UInt32}") {
		t.Error("limit/offset must be parameterized")
	}
	if !strings.Contains(sql, "FROM meter_events FINAL") {
		t.Error("must query meter_events FINAL")
	}
	if !strings.Contains(sql, "ORDER BY event_time DESC") {
		t.Error("must order by event_time DESC")
	}
}

func TestBuildAuditQueryLimitClamped(t *testing.T) {
	// 默认 100
	_, args := buildAuditQuery(AuditFilter{TenantID: "t"})
	if args[len(args)-2] != 100 {
		t.Fatalf("default limit = %v, want 100", args[len(args)-2])
	}
	// 上限 1000
	_, args = buildAuditQuery(AuditFilter{TenantID: "t", Limit: 99999})
	if args[len(args)-2] != 1000 {
		t.Fatalf("limit clamp = %v, want 1000", args[len(args)-2])
	}
	// 负数归一为默认
	_, args = buildAuditQuery(AuditFilter{TenantID: "t", Limit: -5})
	if args[len(args)-2] != 100 {
		t.Fatalf("negative limit = %v, want 100", args[len(args)-2])
	}
}

// 值全部走命名参数绑定，SQL 文本不拼接用户输入（防注入）。
func TestBuildAuditQueryNoInlineValues(t *testing.T) {
	sql, _ := buildAuditQuery(AuditFilter{
		TenantID: "11111111-1111-1111-1111-111111111111",
		Model:    "'; DROP TABLE meter_events; --",
	})
	if strings.Contains(sql, "DROP TABLE") {
		t.Fatalf("user input leaked into SQL: %s", sql)
	}
}

// ---- parseAuditFilter ----

func TestParseAuditFilter(t *testing.T) {
	r := httptest.NewRequest("GET",
		"/api/v1/audit/events?customer_id=22222222-2222-2222-2222-222222222222&task_type=chat&agent_id=a1&model=gpt-4&provider=openai&status_code=429&limit=50&offset=10&from=2026-08-23T00:00:00Z&to=2026-08-24T00:00:00Z",
		nil)
	f, err := parseAuditFilter(r, "11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if f.TenantID != "11111111-1111-1111-1111-111111111111" {
		t.Error("tenant must be server-injected")
	}
	if f.CustomerID != "22222222-2222-2222-2222-222222222222" || f.TaskType != "chat" ||
		f.AgentID != "a1" || f.Model != "gpt-4" || f.Provider != "openai" {
		t.Errorf("filters mismatch: %+v", f)
	}
	if f.StatusCode != 429 || f.Limit != 50 || f.Offset != 10 {
		t.Errorf("numeric filters mismatch: %+v", f)
	}
	if !f.From.Equal(time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("from = %v", f.From)
	}
}

func TestParseAuditFilterDefaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/audit/events", nil)
	f, err := parseAuditFilter(r, "t")
	if err != nil {
		t.Fatal(err)
	}
	// 缺省时间范围：近 24 小时（防无界扫描）
	d := f.To.Sub(f.From)
	if d < 23*time.Hour || d > 25*time.Hour {
		t.Fatalf("default window = %v, want ~24h", d)
	}
	if f.Limit != 0 { // 0 交给 buildAuditQuery 归一为 100
		t.Fatalf("limit = %d, want 0 (normalized later)", f.Limit)
	}
}

func TestParseAuditFilterValidation(t *testing.T) {
	cases := []string{
		"status_code=abc",
		"status_code=-1",
		"status_code=70000",
		"from=not-a-date",
		"to=2026/08/24",
	}
	for _, q := range cases {
		r := httptest.NewRequest("GET", "/api/v1/audit/events?"+q, nil)
		if _, err := parseAuditFilter(r, "t"); err == nil {
			t.Errorf("%s should be rejected", q)
		}
	}
}

func TestParseAuditFilterIgnoresBadLimit(t *testing.T) {
	// 非法 limit/offset 不报错（静默忽略，沿用默认）
	r := httptest.NewRequest("GET", "/api/v1/audit/events?limit=abc&offset=-3", nil)
	f, err := parseAuditFilter(r, "t")
	if err != nil {
		t.Fatalf("bad limit/offset should be ignored, got err: %v", err)
	}
	if f.Limit != 0 || f.Offset != 0 {
		t.Fatalf("filters = %+v", f)
	}
}
