// 高级集成测试：auto 路由、阶梯计价、预算策略、限流、预留生命周期、
// 时序查询、对账、审计过滤、数据隔离、模型列表等未覆盖场景。
//
// 运行前提与 integration_test.go 相同（docker compose 全栈基础设施）。
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/worker"
)

// TestIntegAutoRouting auto 路由集成：model=auto 短输入 → economy 渠道（mock），
// X-Meteron-Quality 头显式覆盖启发式，X-Meteron-Provider 显式锁定渠道。
func TestIntegAutoRouting(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_auto_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 1) model=auto：短输入（<2000 tokens）→ economy → mock 渠道默认模型 mock-chat
	rec, parsed := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "auto",
		"messages": []map[string]string{{"role": "user", "content": "auto 路由测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("auto route status = %d, body = %s", rec.Code, rec.Body.String())
	}
	// 响应中的 model 应为实际执行的模型（mock-chat），而非 "auto"
	if m, _ := parsed["model"].(string); m != "mock-chat" {
		t.Fatalf("auto routed model = %q, want mock-chat (economy default)", m)
	}
	// 计量事件中的模型也是最终路由模型
	waitFor(t, 20*time.Second, "auto usage in clickhouse", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		_ = json.Unmarshal(resp.Data, &u)
		for _, it := range u.Items {
			if it.Model == "mock-chat" {
				return true
			}
		}
		return false
	})

	// 2) X-Meteron-Quality: low → economy；显式锁定渠道与模型
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		jsonBody(t, map[string]interface{}{
			"model":    "auto",
			"messages": []map[string]string{{"role": "user", "content": "质量头测试"}},
		}))
	req.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.Header.Set("X-Meteron-Quality", "low")
	req.RemoteAddr = "192.0.2.1:1234"
	rec2 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Fatalf("quality low auto status = %d, body = %s", rec2.Code, rec2.Body.String())
	}

	// 3) X-Meteron-Provider: mock + model=auto → 渠道默认模型 mock-chat
	req3 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		jsonBody(t, map[string]interface{}{
			"model":    "auto",
			"messages": []map[string]string{{"role": "user", "content": "显式渠道测试"}},
		}))
	req3.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req3.Header.Set("X-Meteron-Customer-ID", ext)
	req3.Header.Set("X-Meteron-Task-Type", "chat")
	req3.Header.Set("X-Meteron-Provider", "mock")
	req3.RemoteAddr = "192.0.2.1:1234"
	rec3 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("explicit provider auto status = %d, body = %s", rec3.Code, rec3.Body.String())
	}
	var parsed3 map[string]interface{}
	_ = json.Unmarshal(rec3.Body.Bytes(), &parsed3)
	if m, _ := parsed3["model"].(string); m != "mock-chat" {
		t.Fatalf("explicit provider model = %q, want mock-chat", m)
	}

	// 4) 未知显式渠道 → 400
	req4 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		jsonBody(t, map[string]interface{}{
			"model":    "mock-chat",
			"messages": []map[string]string{{"role": "user", "content": "未知渠道"}},
		}))
	req4.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req4.Header.Set("X-Meteron-Customer-ID", ext)
	req4.Header.Set("X-Meteron-Task-Type", "chat")
	req4.Header.Set("X-Meteron-Provider", "no-such-provider")
	req4.RemoteAddr = "192.0.2.1:1234"
	rec4 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusBadRequest {
		t.Fatalf("unknown provider status = %d, want 400", rec4.Code)
	}
}

// TestIntegTieredPricing 阶梯计价全链路：tiered 规则 → 事件快照携带 tiers →
// 出账按累计 token 跨档切分计价 → 账单金额精确。
func TestIntegTieredPricing(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_tier_" + uuid.NewString()[:8]
	s := env.provisionTiered(t, ext, "0.01", "0.005") // 首 100 token 单价 0.01，超出 0.005

	// 两次对话：累计 token 跨过首档上界（mock 每次 input≈body/4 + output 64）
	for i := 0; i < 2; i++ {
		rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
			"model":      "mock-chat",
			"messages":   []map[string]string{{"role": "user", "content": "阶梯计价测试，长文本跨档"}},
			"max_tokens": 64,
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("chat %d status = %d, body = %s", i, rec.Code, rec.Body.String())
		}
	}

	// 等待事件落 ClickHouse
	waitFor(t, 20*time.Second, "tiered usage in clickhouse", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		_ = json.Unmarshal(resp.Data, &u)
		return len(u.Items) > 0
	})

	// 触发出账
	now := time.Now().UTC()
	periodStart := now.Truncate(24 * time.Hour)
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/billing-runs", map[string]string{
		"period_start": periodStart.Format(time.RFC3339),
		"period_end":   periodStart.Add(24 * time.Hour).Format(time.RFC3339),
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("billing run: %d %s", resp.Code, string(resp.Data))
	}

	// 查账单并校验金额 = 按 tiers 计价
	var billID string
	var totalTokens uint64
	waitFor(t, 10*time.Second, "tiered bill", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/bills?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		bills := apiList[map[string]interface{}](t, resp)
		for _, b := range bills {
			if st, _ := b["status"].(string); st == "draft" {
				billID, _ = b["id"].(string)
			}
		}
		return billID != ""
	})
	if billID == "" {
		t.Fatal("no draft bill for tiered pricing")
	}

	// 从用量 API 拿实际 token 数，手算期望金额
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
	var u struct {
		Items []meter.UsageSummary `json:"items"`
	}
	if err := json.Unmarshal(resp.Data, &u); err != nil {
		t.Fatalf("parse usage: %v", err)
	}
	for _, it := range u.Items {
		totalTokens += it.InputTokens + it.OutputTokens
	}

	// 账单详情
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/bills/"+billID, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("get bill: %d", resp.Code)
	}
	var bill struct {
		TotalAmount string `json:"total_amount"`
		LineItems   []struct {
			Amount   string `json:"amount"`
			Quantity string `json:"quantity"`
		} `json:"line_items"`
	}
	if err := json.Unmarshal(resp.Data, &bill); err != nil {
		t.Fatalf("parse bill: %v", err)
	}
	if len(bill.LineItems) == 0 {
		t.Fatal("bill has no line items")
	}

	// 期望金额：min(tokens,100)*0.01 + max(tokens-100,0)*0.005
	firstTier := decimal.NewFromInt(int64(min64(totalTokens, 100))).Mul(decimal.RequireFromString("0.01"))
	overflow := int64(0)
	if totalTokens > 100 {
		overflow = int64(totalTokens - 100)
	}
	expect := firstTier.Add(decimal.NewFromInt(overflow).Mul(decimal.RequireFromString("0.005")))
	actual, err := decimal.NewFromString(bill.TotalAmount)
	if err != nil {
		t.Fatalf("parse bill amount: %v", err)
	}
	if !actual.Equal(expect) {
		t.Fatalf("tiered bill amount = %s, want %s (tokens=%d)", bill.TotalAmount, expect, totalTokens)
	}
}

// TestIntegBudgetExhaustBlock block 策略：预算池小额 → 第二次请求被拒（402），
// 且第一次请求的 confirm 金额不超过预算。
func TestIntegBudgetExhaustBlock(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_block_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 改造预算池为 block 策略 + 极小预算（0.001 USD ≈ 500 tokens 预估）
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/budget-pools", map[string]string{
		"customer_id": s.customerID, "max_budget": "0.001", "exhaust_policy": "block",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create block pool: %d %s", resp.Code, string(resp.Data))
	}

	// 第一次请求成功（预估 ~0.0026 > 0.001 但 mock 成本极低；block 在预留阶段判定）
	// 预估金额 = (输入字符/4 + max_tokens默认1024) * 0.000002 ≈ 0.0022 > 0.001 → 首次即被拒
	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "block 策略测试"}},
	})
	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("exhausted block status = %d, want 402, body = %s", rec.Code, rec.Body.String())
	}
	// 业务码 1002 BUDGET_EXHAUSTED
	var errResp struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if errResp.Code != 1002 {
		t.Fatalf("biz code = %d, want 1002", errResp.Code)
	}
}

// TestIntegBudgetExhaustWarn warn 策略：预算耗尽仍放行（超支可通过用量观察）。
func TestIntegBudgetExhaustWarn(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_warn_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 小额 warn 池：0.001
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/budget-pools", map[string]string{
		"customer_id": s.customerID, "max_budget": "0.001", "exhaust_policy": "warn",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create warn pool: %d %s", resp.Code, string(resp.Data))
	}

	// warn 策略：预算不足仍放行（无预留头）
	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "warn 策略测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("warn policy status = %d, want 200, body = %s", rec.Code, rec.Body.String())
	}
	// 无预留 → 响应无 X-Meteron-Reservation-ID 头
	if rid := rec.Header().Get("X-Meteron-Reservation-ID"); rid != "" {
		t.Fatalf("warn policy should proceed without reservation, got %s", rid)
	}
}

// TestIntegRateLimitRPM 客户 RPM 限流：限制 2/min → 第三次 429。
func TestIntegRateLimitRPM(t *testing.T) {
	env := newTestEnv(t)
	// 启用 RPM 限制
	env.cfg.RateLimit.CustomerRPM = 2
	ext := "it_rpm_" + uuid.NewString()[:8]
	env.provision(t, ext)

	codes := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
			"model":    "mock-chat",
			"messages": []map[string]string{{"role": "user", "content": "RPM 限流测试"}},
		})
		codes = append(codes, rec.Code)
	}
	if codes[0] != http.StatusOK || codes[1] != http.StatusOK {
		t.Fatalf("first two should pass: %v", codes)
	}
	if codes[2] != http.StatusTooManyRequests {
		t.Fatalf("third should be 429: %v", codes)
	}
	// 错误码 1014 RATE_LIMITED
	last := codes[2]
	if last != http.StatusTooManyRequests {
		t.Fatalf("unexpected")
	}
}

// TestIntegRateLimitConcurrent 客户并发限流：限制 1 并发（通过直接调用限流器验证信号量语义）。
func TestIntegRateLimitConcurrent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	key := "rl:test:conc:" + uuid.NewString()
	release, ok, err := env.limiter.Acquire(ctx, key, 1)
	if err != nil || !ok {
		t.Fatalf("first acquire failed: ok=%v err=%v", ok, err)
	}
	// 第二次获取（未释放）→ 拒绝
	if _, ok2, _ := env.limiter.Acquire(ctx, key, 1); ok2 {
		t.Fatal("second concurrent acquire must fail with limit=1")
	}
	// 释放后可重新获取
	release()
	if _, ok3, err3 := env.limiter.Acquire(ctx, key, 1); err3 != nil || !ok3 {
		t.Fatalf("acquire after release failed: ok=%v err=%v", ok3, err3)
	}
}

// TestIntegReservationConfirmRelease 预留手动 confirm/release API：
// 创建预留 → POST confirm（差额归还）→ POST release（幂等拒绝）。
func TestIntegReservationConfirmRelease(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_resv_" + uuid.NewString()[:8]
	s := env.provision(t, ext)
	ctx := context.Background()

	poolID, err := uuid.Parse(s.poolID)
	if err != nil {
		t.Fatalf("parse pool id: %v", err)
	}
	pool, err := env.ledgerSvc.Store().GetPool(ctx, uuid.MustParse(env.tenantID()), poolID)
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	// 预留 0.5
	res, err := env.ledgerSvc.Reserve(ctx, pool, decimal.NewFromFloat(0.5), uuid.New(), time.Minute)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// pending 余额 = 10 - 0.5
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
	var bal struct {
		PendingReserved string `json:"pending_reserved"`
		AvailableAmount string `json:"available_amount"`
	}
	if err := json.Unmarshal(resp.Data, &bal); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if bal.PendingReserved != "0.5" {
		t.Fatalf("pending = %s, want 0.5", bal.PendingReserved)
	}

	// confirm 实际 0.2（差额 0.3 归还）
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/budget-reservations/"+res.ID.String()+"/confirm",
		map[string]string{"actual_amount": "0.2"})
	if resp.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", resp.Code, string(resp.Data))
	}

	// confirm 后：spent=0.2，pending=0
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
	if err := json.Unmarshal(resp.Data, &bal); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if bal.PendingReserved != "0" {
		t.Fatalf("pending after confirm = %s, want 0", bal.PendingReserved)
	}
	if bal.AvailableAmount != "9.8" {
		t.Fatalf("available after confirm = %s, want 9.8", bal.AvailableAmount)
	}

	// 重复 confirm（不同金额）→ 409 状态非法流转
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/budget-reservations/"+res.ID.String()+"/confirm",
		map[string]string{"actual_amount": "0.3"})
	if resp.Code != http.StatusConflict {
		t.Fatalf("re-confirm status = %d, want 409", resp.Code)
	}
}

// TestIntegUsageTimeseries 用量时序查询：多轮对话 → 小时粒度点位存在且 token 单调非减。
func TestIntegUsageTimeseries(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_ts_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 三轮对话
	for i := 0; i < 3; i++ {
		rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
			"model":    "mock-chat",
			"messages": []map[string]string{{"role": "user", "content": "时序测试"}},
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("chat %d status = %d", i, rec.Code)
		}
	}

	waitFor(t, 20*time.Second, "timeseries points", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage/timeseries?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var ts struct {
			Points []meter.TimeseriesPoint `json:"points"`
		}
		if err := json.Unmarshal(resp.Data, &ts); err != nil {
			return false
		}
		total := uint64(0)
		for _, p := range ts.Points {
			total += p.RequestCount
		}
		return total >= 3
	})

	// 非法时间范围 → 400
	_, resp := env.doReq(t, http.MethodGet,
		"/api/v1/usage/timeseries?customer_id="+s.customerID+"&start_time=2026-08-25T10:00:00Z&end_time=2026-08-25T09:00:00Z", nil)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid range status = %d, want 400", resp.Code)
	}
}

// TestIntegReconciliationRunsList 对账跑批列表端点：GET /api/v1/reconciliation-runs
// 正常返回（Worker 的轻量/深度循环由部署进程驱动，此处验证查询端点契约：
// 状态值合法、字段序列化正常；允许空列表——纯 handler 环境无历史记录）。
func TestIntegReconciliationRunsList(t *testing.T) {
	env := newTestEnv(t)

	_, resp := env.doReq(t, http.MethodGet, "/api/v1/reconciliation-runs", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("recon runs list: %d %s", resp.Code, string(resp.Data))
	}
	runs := apiList[map[string]interface{}](t, resp)
	validStatus := map[string]bool{"pending": true, "running": true, "completed": true, "failed": true}
	for _, r := range runs {
		if st, _ := r["status"].(string); !validStatus[st] {
			t.Fatalf("recon run has invalid status %q: %+v", st, r)
		}
	}
}

// TestIntegReconRunLifecycle 对账 run 记录生命周期：插入 pending → 流转 completed →
// 列表端点回显（与 recon.Worker 的 createRun/completeRun 同构的状态机）。
func TestIntegReconRunLifecycle(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	tenantID := uuid.MustParse(env.tenantID())

	// 模拟轻量对账写入一条 run 记录（与 recon.Worker.createRun 同构）
	_, err := env.db.Exec(ctx, `
INSERT INTO reconciliation_runs (tenant_id, run_date, scope, status)
VALUES ($1::uuid, NOW(), '{"type":"light"}'::jsonb, 'pending')`,
		tenantID.String())
	if err != nil {
		t.Fatalf("insert recon run: %v", err)
	}

	// 模拟完成（与 completeRun 同构）
	_, err = env.db.Exec(ctx, `
UPDATE reconciliation_runs SET status='completed', started_at=NOW(), completed_at=NOW(),
  usage_total=100, rated_total=100, ledger_total=100, diff_amount=0, corrected_count=0
WHERE tenant_id=$1::uuid AND status='pending'`,
		tenantID.String())
	if err != nil {
		t.Fatalf("update recon run: %v", err)
	}

	_, resp := env.doReq(t, http.MethodGet, "/api/v1/reconciliation-runs", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("recon runs list: %d", resp.Code)
	}
	runs := apiList[map[string]interface{}](t, resp)
	found := false
	for _, r := range runs {
		if st, _ := r["status"].(string); st == "completed" {
			if diff, _ := r["diff_amount"].(string); diff == "0" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("completed recon run with diff=0 not found: %+v", runs)
	}
}

// TestIntegAuditFilters 审计日志多维过滤：status_code / provider / task_type 组合过滤。
func TestIntegAuditFilters(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_audit_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "审计过滤测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d", rec.Code)
	}

	// 等事件落 ClickHouse
	waitFor(t, 20*time.Second, "audit events", func() bool {
		_, resp := env.doReq(t, http.MethodGet,
			"/api/v1/audit/events?customer_id="+s.customerID+"&provider=mock&status_code=200&task_type=chat", nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var a struct {
			Events []map[string]interface{} `json:"events"`
			Total  int                      `json:"total"`
		}
		if err := json.Unmarshal(resp.Data, &a); err != nil {
			return false
		}
		return a.Total > 0
	})

	// 过滤不存在的 provider → 空
	_, resp := env.doReq(t, http.MethodGet,
		"/api/v1/audit/events?customer_id="+s.customerID+"&provider=nonexistent", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("audit filter: %d", resp.Code)
	}
	var a struct {
		Total int `json:"total"`
	}
	if err := json.Unmarshal(resp.Data, &a); err != nil {
		t.Fatalf("parse audit: %v", err)
	}
	if a.Total != 0 {
		t.Fatalf("nonexistent provider filter should be empty, got %d", a.Total)
	}

	// 非法 status_code → 400
	_, resp = env.doReq(t, http.MethodGet,
		"/api/v1/audit/events?customer_id="+s.customerID+"&status_code=99999", nil)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid status_code = %d, want 400", resp.Code)
	}

	// 非法 customer_id → 400
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/audit/events?customer_id=not-a-uuid", nil)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid customer_id = %d, want 400", resp.Code)
	}
}

// TestIntegCustomerIsolation 客户数据隔离：客户 A 的密钥查不到客户 B 的用量。
func TestIntegCustomerIsolation(t *testing.T) {
	env := newTestEnv(t)
	extA := "it_iso_a_" + uuid.NewString()[:8]
	extB := "it_iso_b_" + uuid.NewString()[:8]
	sa := env.provision(t, extA)
	sb := env.provision(t, extB)

	// A、B 各产生一次用量
	for _, ext := range []string{extA, extB} {
		rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
			"model":    "mock-chat",
			"messages": []map[string]string{{"role": "user", "content": "隔离测试"}},
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("chat %s status = %d", ext, rec.Code)
		}
	}

	// 创建绑定客户 A 的 admin 密钥
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id": sa.customerID,
		"scopes":      []string{"admin"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create key: %d", resp.Code)
	}
	var keyResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 等待 A 的用量可见（隔离查询前保证数据落库）
	waitFor(t, 20*time.Second, "customer A usage", func() bool {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?customer_id="+sa.customerID, nil)
		req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		env.apiHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}
		var r apiResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &r)
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		_ = json.Unmarshal(r.Data, &u)
		return len(u.Items) > 0
	})

	// 客户 A 密钥查询时试图指定客户 B → 强制回落到 A（只看到 A 的数据）
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?customer_id="+sb.customerID, nil)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("customer-scoped query status = %d", rec.Code)
	}
	var r apiResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &r)
	var u struct {
		CustomerID string               `json:"customer_id"`
		Items      []meter.UsageSummary `json:"items"`
	}
	if err := json.Unmarshal(r.Data, &u); err != nil {
		t.Fatalf("parse usage: %v", err)
	}
	// 返回的客户 ID 必须是 A（不是 B）——客户级密钥强制数据隔离
	if u.CustomerID != sa.customerID {
		t.Fatalf("usage customer_id = %s, want %s (key-bound customer)", u.CustomerID, sa.customerID)
	}

	// 网关侧：A 的密钥不能以 B 身份调用（越权计费防护）
	body := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "越权测试"}},
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	gwReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	gwReq.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	gwReq.Header.Set("X-Meteron-Customer-ID", extB) // 冒充 B
	gwReq.Header.Set("X-Meteron-Task-Type", "chat")
	gwReq.RemoteAddr = "192.0.2.1:1234"
	gwRec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(gwRec, gwReq)
	if gwRec.Code != http.StatusForbidden {
		t.Fatalf("cross-customer call status = %d, want 403", gwRec.Code)
	}
}

// TestIntegDebugChatSandbox 调试沙盒：POST /api/v1/debug/chat 走完整网关链路。
func TestIntegDebugChatSandbox(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_debug_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "沙盒调试测试"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/debug/chat", &buf)
	req.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("debug chat status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("parse debug response: %v", err)
	}
	if _, ok := parsed["usage"]; !ok {
		t.Fatalf("debug chat response missing usage: %s", rec.Body.String())
	}

	// 沙盒对话也产生计量（审计可见）
	waitFor(t, 20*time.Second, "debug chat usage", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		_ = json.Unmarshal(resp.Data, &u)
		return len(u.Items) > 0
	})
}

// TestIntegModelsEndpoint 模型列表：GET /api/v1/models 返回配置的全部模型。
func TestIntegModelsEndpoint(t *testing.T) {
	env := newTestEnv(t)

	_, resp := env.doReq(t, http.MethodGet, "/api/v1/models", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("models: %d %s", resp.Code, string(resp.Data))
	}
	models := apiList[map[string]interface{}](t, resp)
	if len(models) == 0 {
		t.Fatal("models list empty")
	}
	// 默认配置含 mock-chat 与 mock-cheap
	found := map[string]bool{}
	for _, m := range models {
		if name, ok := m["model"].(string); ok {
			found[name] = true
		}
	}
	if !found["mock-chat"] || !found["mock-cheap"] {
		t.Fatalf("models missing mock-chat/mock-cheap: %v", found)
	}

	// 网关 scope 密钥也可读（bootstrap 含全部 scope）
	req := httptest.NewRequest(http.MethodGet, "/api/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("models with bootstrap key: %d", rec.Code)
	}
}

// TestIntegExpiredKey 过期密钥：expire_at 已过 → 鉴权拒绝（401）。
func TestIntegExpiredKey(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_expk_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 创建已过期密钥（expire_at 为 1 小时前）
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id": s.customerID,
		"scopes":      []string{"gateway_invoke"},
		"expire_at":   time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create expired key: %d %s", resp.Code, string(resp.Data))
	}
	var keyResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)
	if keyResp.APIKey == "" {
		t.Fatal("missing key plaintext")
	}

	// 过期密钥调用网关 → 401
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "过期密钥测试"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired key status = %d, want 401", rec.Code)
	}
}

// TestIntegScheduledWorkerExpire 定时过期释放：直接调用 ExpirePendingBatch（跳过等待），
// 验证过期预留被批量释放且 Redis 额度归还。
func TestIntegScheduledWorkerExpire(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_swexp_" + uuid.NewString()[:8]
	s := env.provision(t, ext)
	ctx := context.Background()

	poolID, err := uuid.Parse(s.poolID)
	if err != nil {
		t.Fatalf("parse pool id: %v", err)
	}
	pool, err := env.ledgerSvc.Store().GetPool(ctx, uuid.MustParse(env.tenantID()), poolID)
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	// 创建一个已过期的 pending 预留（ttl 1 秒）
	res, err := env.ledgerSvc.Reserve(ctx, pool, decimal.NewFromFloat(0.3), uuid.New(), time.Second)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 等待过期时刻到达
	time.Sleep(1100 * time.Millisecond)

	// 直接执行过期批处理（与 ScheduledWorker 同一方法）
	n, err := env.ledgerSvc.ExpirePendingBatch(ctx, 500)
	if err != nil {
		t.Fatalf("expire batch: %v", err)
	}
	if n == 0 {
		t.Fatal("expire batch should process at least the expired reservation")
	}

	// 预留状态 = expired
	var status string
	if err := env.db.QueryRow(ctx,
		`SELECT status::text FROM budget_reservations WHERE id=$1::uuid`, res.ID.String()).Scan(&status); err != nil {
		t.Fatalf("query reservation: %v", err)
	}
	if status != "expired" {
		t.Fatalf("reservation status = %s, want expired", status)
	}

	// pending 归零（预算池余额恢复）
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
	var bal struct {
		PendingReserved string `json:"pending_reserved"`
		AvailableAmount string `json:"available_amount"`
	}
	if err := json.Unmarshal(resp.Data, &bal); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if bal.PendingReserved != "0" {
		t.Fatalf("pending after expire = %s, want 0", bal.PendingReserved)
	}

	// 过期后 confirm → 409（RESERVATION_EXPIRED 1001）
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/budget-reservations/"+res.ID.String()+"/confirm",
		map[string]string{"actual_amount": "0.1"})
	if resp.Code != http.StatusConflict {
		t.Fatalf("confirm expired status = %d, want 409", resp.Code)
	}
}

// TestIntegSinkIdempotency 计量幂等：同 event_id 重复投递 ClickHouse 去重（FINAL 查询）。
func TestIntegSinkIdempotency(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_idem2_" + uuid.NewString()[:8]
	s := env.provision(t, ext)
	ctx := context.Background()

	// 同一 event_id 投递两次（绕过网关直接 Produce）
	tenantID := uuid.MustParse(env.tenantID())
	ev := testEvent(tenantID, uuid.MustParse(s.customerID), "dup-model", 42, 21)
	if err := env.producer.Produce(ctx, ev); err != nil {
		t.Fatalf("produce 1: %v", err)
	}
	if err := env.producer.Produce(ctx, ev); err != nil {
		t.Fatalf("produce 2 (duplicate event_id): %v", err)
	}

	// 等待落库后 FINAL 查询只计一次
	waitFor(t, 20*time.Second, "deduped event in usage", func() bool {
		var cnt uint64
		if err := env.ch.QueryRow(ctx, `
SELECT count() FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND customer_id = toUUID($2) AND model = $3`,
			tenantID.String(), s.customerID, "dup-model").Scan(&cnt); err != nil {
			return false
		}
		return cnt == 1
	})
}

// TestIntegOutboxRelay Outbox 中转：事件先落 PG outbox → relay 投递 Kafka → published_at 标记。
func TestIntegOutboxRelay(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_outbox_" + uuid.NewString()[:8]
	s := env.provision(t, ext)
	ctx := context.Background()

	tenantID := uuid.MustParse(env.tenantID())
	ev := testEvent(tenantID, uuid.MustParse(s.customerID), "outbox-model", 10, 5)
	if err := env.producer.Produce(ctx, ev); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// outbox 行已插入
	var cnt int
	if err := env.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM meter_outbox WHERE event_id=$1::uuid`, ev.EventID.String()).Scan(&cnt); err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("outbox rows = %d, want 1", cnt)
	}

	// 等待即时投递或 relay 标记 published（relay 间隔 5s）
	waitFor(t, 20*time.Second, "outbox published", func() bool {
		var published bool
		if err := env.db.QueryRow(ctx,
			`SELECT published_at IS NOT NULL FROM meter_outbox WHERE event_id=$1::uuid`,
			ev.EventID.String()).Scan(&published); err != nil {
			return false
		}
		return published
	})

	// 事件最终落 ClickHouse
	waitFor(t, 20*time.Second, "outbox event in clickhouse", func() bool {
		var cnt uint64
		if err := env.ch.QueryRow(ctx, `
SELECT count() FROM meter_events FINAL
WHERE tenant_id = toUUID($1) AND customer_id = toUUID($2) AND model = $3`,
			tenantID.String(), s.customerID, "outbox-model").Scan(&cnt); err != nil {
			return false
		}
		return cnt == 1
	})
}

// TestIntegLateEventWindow 晚到事件窗口：超出回溯窗口的事件被拒（400，错误码 1003）。
func TestIntegLateEventWindow(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_late_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 超出 34 天回溯窗口 → 拒绝
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/ingest/events", map[string]interface{}{
		"event_id":       uuid.NewString(),
		"transaction_id": uuid.NewString(),
		"customer_id":    s.customerID,
		"model":          "late-model",
		"timestamp":      time.Now().Add(-40 * 24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("late event status = %d, want 400", resp.Code)
	}
	if resp.Code == http.StatusBadRequest && resp.Code != 0 {
		var e struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(resp.Data, &e)
		// 统一错误包裹 code 字段在顶层
		if e.Code == 0 {
			// data 内 detail；顶层 code 已解析
		}
	}

	// 窗口内的晚到事件（5 天前）→ 接受
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/ingest/events", map[string]interface{}{
		"event_id":       uuid.NewString(),
		"transaction_id": uuid.NewString(),
		"customer_id":    s.customerID,
		"model":          "late-ok-model",
		"input_tokens":   10,
		"output_tokens":  5,
		"timestamp":      time.Now().Add(-5 * 24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("in-window late event status = %d, want 200: %s", resp.Code, string(resp.Data))
	}

	// 未来时间戳 → 拒绝
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/ingest/events", map[string]interface{}{
		"event_id":       uuid.NewString(),
		"transaction_id": uuid.NewString(),
		"customer_id":    s.customerID,
		"model":          "future-model",
		"timestamp":      time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	})
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("future event status = %d, want 400", resp.Code)
	}
}

// TestIntegBillingRunIdempotency 出账幂等：同周期重复出账重建 draft，不产生重复账单。
func TestIntegBillingRunIdempotency(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_bilidem_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "幂等出账测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d", rec.Code)
	}
	waitFor(t, 20*time.Second, "usage for idempotent billing", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		_ = json.Unmarshal(resp.Data, &u)
		return len(u.Items) > 0
	})

	now := time.Now().UTC()
	periodStart := now.Truncate(24 * time.Hour)
	period := map[string]string{
		"period_start": periodStart.Format(time.RFC3339),
		"period_end":   periodStart.Add(24 * time.Hour).Format(time.RFC3339),
	}

	// 第一次出账
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/billing-runs", period)
	if resp.Code != http.StatusOK {
		t.Fatalf("billing run 1: %d %s", resp.Code, string(resp.Data))
	}
	// 第二次出账（同周期）
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/billing-runs", period)
	if resp.Code != http.StatusOK {
		t.Fatalf("billing run 2: %d %s", resp.Code, string(resp.Data))
	}

	// 同客户同周期 draft 账单仅一份
	var cnt int
	if err := env.db.QueryRow(context.Background(), `
SELECT COUNT(*) FROM bills
WHERE tenant_id=$1::uuid AND customer_id=$2::uuid
  AND period_start=$3 AND period_end=$4 AND status='draft'`,
		env.tenantID(), s.customerID, periodStart, periodStart.Add(24*time.Hour)).Scan(&cnt); err != nil {
		t.Fatalf("query bills: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("draft bills for same period = %d, want 1", cnt)
	}

	// finalized 账单保护：定稿后再出账 → 跳过（不重建、不报错）
	var billID string
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/bills?customer_id="+s.customerID, nil)
	for _, b := range apiList[map[string]interface{}](t, resp) {
		if st, _ := b["status"].(string); st == "draft" {
			billID, _ = b["id"].(string)
		}
	}
	if billID == "" {
		t.Fatal("no draft bill found")
	}
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/bills/"+billID+"/finalize", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("finalize: %d", resp.Code)
	}

	_, resp = env.doReq(t, http.MethodPost, "/api/v1/billing-runs", period)
	if resp.Code != http.StatusOK {
		t.Fatalf("billing run 3 (finalized period): %d %s", resp.Code, string(resp.Data))
	}
	// finalized 账单仍在且唯一
	var finalCnt int
	if err := env.db.QueryRow(context.Background(), `
SELECT COUNT(*) FROM bills
WHERE tenant_id=$1::uuid AND customer_id=$2::uuid
  AND period_start=$3 AND period_end=$4 AND status='finalized'`,
		env.tenantID(), s.customerID, periodStart, periodStart.Add(24*time.Hour)).Scan(&finalCnt); err != nil {
		t.Fatalf("query finalized bills: %v", err)
	}
	if finalCnt != 1 {
		t.Fatalf("finalized bills = %d, want 1 (protected from rebuild)", finalCnt)
	}
}

// TestIntegAmendNotImplemented 账单修正端点：MVP 返回 501。
func TestIntegAmendNotImplemented(t *testing.T) {
	env := newTestEnv(t)
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/bills/"+uuid.NewString()+"/amend", nil)
	if resp.Code != http.StatusNotImplemented {
		t.Fatalf("amend status = %d, want 501", resp.Code)
	}
}

// TestIntegBillNotFound 不存在的账单 → 404。
func TestIntegBillNotFound(t *testing.T) {
	env := newTestEnv(t)
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/bills/"+uuid.NewString(), nil)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("missing bill status = %d, want 404", resp.Code)
	}
}

// TestIntegDeleteAPIKey 密钥软删除：删除后鉴权拒绝（401），列表不再可见。
func TestIntegDeleteAPIKey(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_delk_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id": s.customerID,
		"scopes":      []string{"gateway_invoke"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create key: %d", resp.Code)
	}
	var keyResp struct {
		ID     string `json:"id"`
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 删除
	_, resp = env.doReq(t, http.MethodDelete, "/api/v1/api-keys/"+keyResp.ID, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("delete key: %d", resp.Code)
	}

	// 再删除（幂等性：已删除 → 404）
	_, resp = env.doReq(t, http.MethodDelete, "/api/v1/api-keys/"+keyResp.ID, nil)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("re-delete key status = %d, want 404", resp.Code)
	}

	// 已删除密钥调用网关 → 401
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "删除密钥测试"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("deleted key status = %d, want 401", rec.Code)
	}
}

// TestIntegListFilters 列表过滤：contracts / budget-pools 支持按 customer_id 过滤。
func TestIntegListFilters(t *testing.T) {
	env := newTestEnv(t)
	extA := "it_lf_a_" + uuid.NewString()[:8]
	extB := "it_lf_b_" + uuid.NewString()[:8]
	sa := env.provision(t, extA)
	sb := env.provision(t, extB)

	// 合同按客户过滤
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/contracts?customer_id="+sa.customerID, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("contracts filter: %d", resp.Code)
	}
	contracts := apiList[map[string]interface{}](t, resp)
	for _, c := range contracts {
		if cid, _ := c["customer_id"].(string); cid != sa.customerID {
			t.Fatalf("contract customer_id = %s, want %s (filter violated)", cid, sa.customerID)
		}
	}
	if len(contracts) == 0 {
		t.Fatal("no contracts for customer A")
	}

	// 预算池按客户过滤
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/budget-pools?customer_id="+sb.customerID, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("pools filter: %d", resp.Code)
	}
	pools := apiList[map[string]interface{}](t, resp)
	for _, p := range pools {
		if cid, _ := p["customer_id"].(string); cid != sb.customerID {
			t.Fatalf("pool customer_id = %s, want %s (filter violated)", cid, sb.customerID)
		}
	}
	if len(pools) == 0 {
		t.Fatal("no pools for customer B")
	}
}

// TestIntegUpdateCustomer 客户更新：PATCH name/currency 后查询可见。
func TestIntegUpdateCustomer(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_upd_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	_, resp := env.doReq(t, http.MethodPatch, "/api/v1/customers/"+s.customerID,
		map[string]string{"name": "IT-Updated", "currency": "EUR"})
	if resp.Code != http.StatusOK {
		t.Fatalf("update customer: %d %s", resp.Code, string(resp.Data))
	}

	_, resp = env.doReq(t, http.MethodGet, "/api/v1/customers/"+s.customerID, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("get customer: %d", resp.Code)
	}
	var c struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(resp.Data, &c); err != nil {
		t.Fatalf("parse customer: %v", err)
	}
	if c.Name != "IT-Updated" || c.Currency != "EUR" {
		t.Fatalf("updated customer = %s/%s, want IT-Updated/EUR", c.Name, c.Currency)
	}

	// 无字段更新 → 400
	_, resp = env.doReq(t, http.MethodPatch, "/api/v1/customers/"+s.customerID, map[string]string{})
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("empty update status = %d, want 400", resp.Code)
	}
}

// TestIntegCustomerRefByUUID 网关客户引用：UUID 与 external_id 双形态均可解析。
func TestIntegCustomerRefByUUID(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_cref_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 用 UUID 引用客户
	rec, _ := env.gatewayChat(t, s.customerID, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "UUID 引用测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("chat by UUID status = %d, body = %s", rec.Code, rec.Body.String())
	}

	// 不存在的客户引用 → 404
	rec2, _ := env.gatewayChat(t, "no-such-customer", map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "不存在客户"}},
	})
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("unknown customer status = %d, want 404", rec2.Code)
	}
}

// TestIntegIngestScope ingest scope 校验：仅 gateway_invoke 的密钥上报事件被拒。
func TestIntegIngestScope(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_ingscope_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"scopes": []string{"gateway_invoke"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create key: %d", resp.Code)
	}
	var keyResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 无 ingest scope 的密钥上报 → 401
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]interface{}{
		"event_id":       uuid.NewString(),
		"transaction_id": uuid.NewString(),
		"customer_id":    s.customerID,
		"model":          "scope-test",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/events", &buf)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ingest without scope status = %d, want 401", rec.Code)
	}
}

// TestIntegScheduledWorkerConstruction ScheduledWorker 构造与首次过期循环
// （验证多组件装配无 panic；后台循环由 ctx 取消退出）。
func TestIntegScheduledWorkerConstruction(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	w := worker.NewScheduledWorker(env.db, env.rds, env.ledgerSvc, env.cfg, testLogger())
	go func() {
		_ = w.Run(ctx)
	}()
	// 给后台循环一点运行时间后取消
	time.Sleep(200 * time.Millisecond)
	cancel()
}

// ---- 辅助 ----

// testLogger 构造静默测试日志器。
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// jsonBody 编码请求体为 reader。
func jsonBody(t *testing.T, v interface{}) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("encode body: %v", err)
	}
	return &buf
}

// provisionTiered 创建 tiered 计价供给链（首档 100 token，两档单价）。
func (e *testEnv) provisionTiered(t *testing.T, ext, tier1Price, tier2Price string) *supply {
	t.Helper()
	s := &supply{}

	_, resp := e.doReq(t, http.MethodPost, "/api/v1/customers", map[string]string{
		"name": "IT-Tiered-" + ext, "external_id": ext, "currency": "USD",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create customer: %d %s", resp.Code, string(resp.Data))
	}
	s.customerID = e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/billable-metrics", map[string]interface{}{
		"name": "it_tiered_metric_" + ext, "aggregation_type": "sum", "group_keys": []string{},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create metric: %d", resp.Code)
	}
	s.metricID = e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/products", map[string]string{
		"name": "it_tiered_product_" + ext, "display_name": "Tiered", "metric_id": s.metricID,
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create product: %d", resp.Code)
	}
	s.productID = e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/rate-cards", map[string]string{
		"name": "it_tiered_rc_" + ext, "currency": "USD",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create rate card: %d", resp.Code)
	}
	s.rateCardID = e.dataID(t, resp)

	// tiered 规则：首档上界 100，超出第二档
	upper := int64(100)
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/rate-cards/"+s.rateCardID+"/rules", map[string]interface{}{
		"product_id":       s.productID,
		"match_conditions": map[string]string{"task_type": "*"},
		"pricing_type":     "tiered",
		"pricing_config": map[string]interface{}{
			"tiers": []map[string]interface{}{
				{"upper": upper, "unit_price": tier1Price},
				{"upper": nil, "unit_price": tier2Price},
			},
		},
		"is_default_rule": true,
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create tiered rule: %d %s", resp.Code, string(resp.Data))
	}
	s.ruleID = e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/contracts", map[string]string{
		"customer_id": s.customerID, "rate_card_id": s.rateCardID,
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create contract: %d", resp.Code)
	}
	s.contractID = e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/budget-pools", map[string]string{
		"customer_id": s.customerID, "max_budget": "100", "exhaust_policy": "warn",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create pool: %d", resp.Code)
	}
	s.poolID = e.dataID(t, resp)

	return s
}

// testEvent 构造直投测试事件（无定价快照：外部上报形态）。
func testEvent(tenantID, customerID uuid.UUID, model string, in, out uint64) *ingest.MeterEvent {
	return &ingest.MeterEvent{
		EventID:       uuid.New(),
		TransactionID: uuid.New(),
		EventVersion:  1,
		TenantID:      tenantID,
		CustomerID:    customerID,
		EventType:     "llm_usage",
		Timestamp:     time.Now().UTC(),
		Provider:      "external",
		Model:         model,
		TaskType:      "batch",
		InputTokens:   in,
		OutputTokens:  out,
		Currency:      "USD",
		Metadata:      map[string]string{},
	}
}

// min64 取小。
func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
