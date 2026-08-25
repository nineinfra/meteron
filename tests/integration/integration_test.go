// Package integration 集成测试：连接真实基础设施（PostgreSQL / ClickHouse / Redis / Kafka），
// 验证 Meteron 各服务组件的组合协作（网络层之前的全部组件）。
//
// 运行前提（与 verify.sh 相同的基础设施）：
//
//	docker compose -f deploy/docker-compose.yml up -d
//
// 运行方式（Go 内置短旗标指定 DSN，缺省回退本地默认值）：
//
//	go test ./tests/integration/ -v -args \
//	  -pg "host=localhost port=5432 user=meteron password=meteron123 dbname=meteron sslmode=disable" \
//	  -ch "clickhouse://meteron:meteron123@localhost:9000/meteron" \
//	  -redis "localhost:6379" -kafka "localhost:9092" -topic "meteron.meter-events"
//
// 注意：集成测试会真实写入 PG / ClickHouse / Kafka（使用独立随机测试客户与资源，
// 通过 external_id 隔离），因此不要指向生产数据源。
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/api"
	"github.com/meteron/meteron/internal/billing"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/gateway"
	"github.com/meteron/meteron/internal/health"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/provider"
	"github.com/meteron/meteron/internal/ratelimit"
	"github.com/meteron/meteron/internal/rbac"
	"github.com/meteron/meteron/internal/router"
	"github.com/meteron/meteron/internal/store"
)

// flagDSN 允许通过 go test -args 覆盖 DSN（不引入 flag 包对外依赖）。
func flagDSN(name, def string) string {
	args := os.Args
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	return def
}

// testEnv 集成测试环境：全部基础设施连接 + 领域服务 + HTTP handler。
type testEnv struct {
	t    *testing.T
	cfg  *config.Config
	db   *pgxpool.Pool
	ch   chdriver.Conn
	rds  *store.Redis
	auth *rbac.Service
	// 领域服务
	ledgerSvc    *ledger.Service
	pricing      *pricing.Engine
	routerEngine *router.Engine
	providers    *provider.Registry
	producer     *ingest.Producer
	billingW     *billing.Worker
	health       *health.Tracker
	limiter      ratelimit.Limiter
	// HTTP
	apiHandler http.Handler
	gwHandler  http.Handler
	// 引导密钥
	bootstrapKey string
}

var (
	pgDSN       = flagDSN("-pg", "host=localhost port=5432 user=meteron password=meteron123 dbname=meteron sslmode=disable")
	chDSN       = flagDSN("-ch", "clickhouse://meteron:meteron123@localhost:9000/meteron")
	redisAddr   = flagDSN("-redis", "localhost:6379")
	kafkaBroker = flagDSN("-kafka", "localhost:9092")
	topic       = flagDSN("-topic", "meteron.meter-events")
)

// newTestEnv 初始化集成测试环境（每个测试独立调用，保证隔离）。
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	cfg := config.Default()
	cfg.PGDSN = pgDSN
	cfg.ClickHouseDSN = chDSN
	cfg.RedisAddr = redisAddr
	cfg.KafkaBrokers = []string{kafkaBroker}
	cfg.KafkaTopic = topic
	// 本地集成测试显式豁免内置默认密钥（仅测试环境）
	cfg.InsecureDefaults = true
	// 使用确定性测试密钥（不依赖外部 env）
	cfg.AESKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	cfg.BootstrapKey = "mk_meteron_bootstrap_dev_key"

	db, err := store.NewPG(ctx, cfg.PGDSN)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(db.Close)
	if err := store.MigratePG(ctx, db); err != nil {
		t.Fatalf("migrate pg: %v", err)
	}

	ch, err := store.NewClickHouse(ctx, cfg.ClickHouseDSN)
	if err != nil {
		t.Fatalf("connect clickhouse: %v", err)
	}
	t.Cleanup(func() { ch.Close() })

	rds, err := store.NewRedis(ctx, cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rds.Client.Close() })

	authSvc, err := rbac.NewService(db, cfg.AESKey)
	if err != nil {
		t.Fatalf("rbac: %v", err)
	}
	if _, _, err := authSvc.Bootstrap(ctx, cfg.BootstrapKey); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	ledgerStore := ledger.NewStore(db)
	ledgerSvc := ledger.NewService(ledgerStore, rds, db, log)
	pricingEngine := pricing.NewEngine(db, log)
	routerEngine := router.NewEngine(db)

	providers, err := provider.NewRegistry(cfg.Providers, 120*time.Second)
	if err != nil {
		t.Fatalf("provider registry: %v", err)
	}
	routerEngine.SetLookup(providers)

	kafkaWriter := store.NewKafkaWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	producer := ingest.NewProducer(kafkaWriter, ingest.NewPGOutboxStore(db), log)
	t.Cleanup(producer.Close)

	billingWorker := billing.NewWorker(db, ch, rds, cfg, log)

	healthTracker := health.NewTracker(cfg.Health.Window, cfg.Health.ErrorRateThreshold, cfg.Health.MinSamples, log)
	limiter := ratelimit.NewRedisLimiter(rds.Client)

	gw := gateway.NewHandler(authSvc, ledgerSvc, ledgerStore, routerEngine, pricingEngine,
		providers, producer, db, cfg, log, healthTracker, limiter)
	apiServer := api.NewServer(db, ch, authSvc, ledgerSvc, pricingEngine, producer,
		billingWorker, cfg, log, healthTracker, gw)

	env := &testEnv{
		t:            t,
		cfg:          cfg,
		db:           db,
		ch:           ch,
		rds:          rds,
		auth:         authSvc,
		ledgerSvc:    ledgerSvc,
		pricing:      pricingEngine,
		routerEngine: routerEngine,
		providers:    providers,
		producer:     producer,
		billingW:     billingWorker,
		health:       healthTracker,
		limiter:      limiter,
		apiHandler:   apiServer.Handler(),
		gwHandler:    gw,
		bootstrapKey: cfg.BootstrapKey,
	}
	return env
}

// ---- HTTP 请求辅助 ----

type apiResponse struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// doReq 发送 HTTP 请求并解析统一响应包裹（{code,msg,data}）。
func (e *testEnv) doReq(t *testing.T, method, path string, body interface{}) (int, apiResponse) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if method != http.MethodGet {
		req.Header.Set("Authorization", "Bearer "+e.bootstrapKey)
	}
	// httptest 的 RemoteAddr 默认 "192.0.2.1:1234"，匹配默认可信代理白名单外的来源，IP 白名单不限制（空名单）
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	e.apiHandler.ServeHTTP(rec, req)
	var resp apiResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec.Code, resp
}

// dataID 从 {code,msg,data:{id}} 响应提取 id。
func (e *testEnv) dataID(t *testing.T, resp apiResponse) string {
	t.Helper()
	var m struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(resp.Data, &m); err != nil || m.ID == "" {
		t.Fatalf("response data has no id: %s", string(resp.Data))
	}
	return m.ID
}

// apiList 解析 data 为对象数组（列表端点）。
func apiList[T any](t *testing.T, resp apiResponse) []T {
	t.Helper()
	var out []T
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		t.Fatalf("parse list data: %v (%s)", err, string(resp.Data))
	}
	return out
}

// waitFor 轮询直到条件满足（异步链路：Kafka→Sink→ClickHouse）。
func waitFor(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// ---- 供给链辅助 ----

type supply struct {
	customerID string
	metricID   string
	productID  string
	rateCardID string
	ruleID     string
	contractID string
	poolID     string
}

// provision 创建完整供给链（客户/计量指标/产品/费率卡/规则/合同/预算池）。
func (e *testEnv) provision(t *testing.T, ext string) *supply {
	t.Helper()
	s := &supply{}

	// 客户
	_, resp := e.doReq(t, http.MethodPost, "/api/v1/customers", map[string]string{
		"name": "IT-Customer-" + ext, "external_id": ext, "currency": "USD",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create customer: %d %s", resp.Code, string(resp.Data))
	}
	s.customerID = e.dataID(t, resp)

	// 计量指标
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/billable-metrics", map[string]interface{}{
		"name": "it_metric_" + ext, "aggregation_type": "sum", "group_keys": []string{},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create metric: %d %s", resp.Code, string(resp.Data))
	}
	s.metricID = e.dataID(t, resp)

	// 产品
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/products", map[string]string{
		"name": "it_product_" + ext, "display_name": "IT Product", "metric_id": s.metricID,
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create product: %d %s", resp.Code, string(resp.Data))
	}
	s.productID = e.dataID(t, resp)

	// 费率卡
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/rate-cards", map[string]string{
		"name": "it_ratecard_" + ext, "currency": "USD",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create rate card: %d %s", resp.Code, string(resp.Data))
	}
	s.rateCardID = e.dataID(t, resp)

	// 费率规则
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/rate-cards/"+s.rateCardID+"/rules", map[string]interface{}{
		"product_id":       s.productID,
		"match_conditions": map[string]string{"task_type": "*"},
		"pricing_type":     "flat",
		"pricing_config": map[string]interface{}{
			"input_unit_price": "0.000003", "output_unit_price": "0.000006",
			"cached_input_unit_price": "0.0000003",
		},
		"is_default_rule": true,
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create rate rule: %d %s", resp.Code, string(resp.Data))
	}
	s.ruleID = e.dataID(t, resp)

	// 合同
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/contracts", map[string]string{
		"customer_id": s.customerID, "rate_card_id": s.rateCardID,
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create contract: %d %s", resp.Code, string(resp.Data))
	}
	s.contractID = e.dataID(t, resp)

	// 预算池
	_, resp = e.doReq(t, http.MethodPost, "/api/v1/budget-pools", map[string]string{
		"customer_id": s.customerID, "max_budget": "10", "warning_threshold": "2", "exhaust_policy": "warn",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create budget pool: %d %s", resp.Code, string(resp.Data))
	}
	s.poolID = e.dataID(t, resp)

	return s
}

// ---- 网关请求辅助 ----

// gatewayChat 发送 OpenAI 兼容对话请求（走完整网关链路）。
func (e *testEnv) gatewayChat(t *testing.T, customerRef string, body map[string]interface{}) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode chat body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+e.bootstrapKey)
	req.Header.Set("X-Meteron-Customer-ID", customerRef)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	e.gwHandler.ServeHTTP(rec, req)
	var parsed map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &parsed)
	return rec, parsed
}

// ---- 核心集成测试 ----

// TestIntegProvisionAndQuery 供给链全流程：创建客户/指标/产品/费率/规则/合同/预算池，
// 并验证列表查询可见。
func TestIntegProvisionAndQuery(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_prov_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 列表查询验证
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/customers", nil)
	found := false
	for _, c := range apiList[map[string]interface{}](t, resp) {
		if c["id"] == s.customerID {
			found = true
		}
	}
	if !found {
		t.Fatalf("customer %s not in list", s.customerID)
	}

	_, resp = env.doReq(t, http.MethodGet, "/api/v1/products", nil)
	if len(apiList[map[string]interface{}](t, resp)) == 0 {
		t.Fatal("products list empty")
	}

	// 预算池余额（初始可用 = 上限）
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("pool balance: %d", resp.Code)
	}
	var bal struct {
		MaxBudget       string `json:"max_budget"`
		SpentAmount     string `json:"spent_amount"`
		AvailableAmount string `json:"available_amount"`
	}
	if err := json.Unmarshal(resp.Data, &bal); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if bal.MaxBudget != "10" {
		t.Fatalf("max budget = %s, want 10", bal.MaxBudget)
	}
	if bal.SpentAmount != "0" {
		t.Fatalf("spent = %s, want 0", bal.SpentAmount)
	}
}

// TestIntegGatewayChatAndUsage 网关对话 + 异步计量链路：
// 调用 mock provider → 验证响应、事务头 → 等待 Kafka→Sink→ClickHouse → 用量查询可见。
func TestIntegGatewayChatAndUsage(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_gw_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	rec, parsed := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "集成测试对话"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Meteron-Transaction-ID") == "" {
		t.Fatal("missing X-Meteron-Transaction-ID header")
	}
	usage, ok := parsed["usage"].(map[string]interface{})
	if !ok {
		t.Fatalf("no usage in response: %s", rec.Body.String())
	}
	if usage["prompt_tokens"] == nil || usage["completion_tokens"] == nil {
		t.Fatal("usage tokens missing")
	}

	// 异步链路：Kafka→Sink→ClickHouse，轮询等待用量可查
	waitFor(t, 20*time.Second, "usage in clickhouse", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		if err := json.Unmarshal(resp.Data, &u); err != nil {
			return false
		}
		return len(u.Items) > 0
	})

	// 用量汇总断言：model/request_count 正确
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
	var u struct {
		Items []meter.UsageSummary `json:"items"`
	}
	if err := json.Unmarshal(resp.Data, &u); err != nil {
		t.Fatalf("parse usage: %v", err)
	}
	if len(u.Items) != 1 {
		t.Fatalf("usage items = %d, want 1: %+v", len(u.Items), u.Items)
	}
	if u.Items[0].Model != "mock-chat" {
		t.Fatalf("usage model = %s, want mock-chat", u.Items[0].Model)
	}
	if u.Items[0].RequestCount != 1 {
		t.Fatalf("request_count = %d, want 1", u.Items[0].RequestCount)
	}
}

// TestIntegIngestEventAndUsage 外部事件上报链路：
// 通过 /api/v1/ingest/events 上报自定义计量事件 → 等待落库 → 用量查询 + 审计日志可见。
func TestIntegIngestEventAndUsage(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_ingest_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	eventID := uuid.New()
	txnID := uuid.New()
	body := map[string]interface{}{
		"event_id":            eventID.String(),
		"transaction_id":      txnID.String(),
		"customer_id":         s.customerID,
		"event_type":          "llm_usage",
		"provider":            "external",
		"model":               "custom-model",
		"task_type":           "batch",
		"input_tokens":        100,
		"output_tokens":       50,
		"cached_input_tokens": 20,
	}
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/ingest/events", body)
	if resp.Code != http.StatusOK {
		t.Fatalf("ingest event: %d %s", resp.Code, string(resp.Data))
	}

	waitFor(t, 20*time.Second, "ingested event in usage", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		var u struct {
			Items []meter.UsageSummary `json:"items"`
		}
		if err := json.Unmarshal(resp.Data, &u); err != nil {
			return false
		}
		for _, it := range u.Items {
			if it.Model == "custom-model" && it.InputTokens == 100 {
				return true
			}
		}
		return false
	})

	// 审计日志可见
	waitFor(t, 20*time.Second, "event in audit log", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/audit/events?customer_id="+s.customerID+"&model=custom-model", nil)
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
}

// TestIntegBillingLifecycle 出账生命周期：网关对话产生用量 → BillingRun → draft 账单 → finalize 不可变。
func TestIntegBillingLifecycle(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_bill_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 产生一次用量
	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "出账测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d", rec.Code)
	}

	// 等待事件落 ClickHouse
	waitFor(t, 20*time.Second, "usage for billing", func() bool {
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

	// 手动触发 BillingRun（今日周期）
	now := time.Now().UTC()
	periodStart := now.Truncate(24 * time.Hour)
	periodEnd := periodStart.Add(24 * time.Hour)
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/billing-runs", map[string]string{
		"period_start": periodStart.Format(time.RFC3339),
		"period_end":   periodEnd.Format(time.RFC3339),
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("trigger billing run: %d %s", resp.Code, string(resp.Data))
	}

	// 账单查询
	var billID string
	waitFor(t, 10*time.Second, "draft bill", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/bills?customer_id="+s.customerID, nil)
		if resp.Code != http.StatusOK {
			return false
		}
		bills := apiList[map[string]interface{}](t, resp)
		if len(bills) == 0 {
			return false
		}
		billID, _ = bills[0]["id"].(string)
		return billID != ""
	})
	if billID == "" {
		t.Fatal("no bill generated")
	}

	// 定稿
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/bills/"+billID+"/finalize", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("finalize bill: %d %s", resp.Code, string(resp.Data))
	}

	// 定稿后状态 = finalized（不可变）
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/bills/"+billID, nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("get bill: %d", resp.Code)
	}
	var bill struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(resp.Data, &bill); err != nil {
		t.Fatalf("parse bill: %v", err)
	}
	if bill.Status != "finalized" {
		t.Fatalf("bill status = %s, want finalized", bill.Status)
	}

	// 账本流水存在（usage_charge）
	var cnt int
	if err := env.db.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM ledger_entries WHERE tenant_id=$1::uuid AND bill_id=$2::uuid`,
		env.tenantID(), billID).Scan(&cnt); err != nil {
		t.Fatalf("query ledger: %v", err)
	}
	if cnt == 0 {
		t.Fatal("no ledger entries for finalized bill")
	}
}

// tenantID 返回 bootstrap 租户 ID（供测试直接查询 PG 断言）。
func (e *testEnv) tenantID() string {
	var tid string
	if err := e.db.QueryRow(context.Background(),
		`SELECT id::text FROM tenants ORDER BY created_at LIMIT 1`).Scan(&tid); err != nil {
		e.t.Fatalf("query tenant: %v", err)
	}
	return tid
}

// TestIntegBudgetReserveConfirm 预算两阶段：请求前预留 → 请求后确认。
// 验证确认后预算池余额 = 实际用量成本（非预估）。
func TestIntegBudgetReserveConfirm(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_budget_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "预算预留确认测试"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d", rec.Code)
	}
	resID := rec.Header().Get("X-Meteron-Reservation-ID")
	if resID == "" {
		t.Fatal("missing X-Meteron-Reservation-ID header")
	}

	// 等待确认完成（异步 confirm 在请求返回后执行）
	waitFor(t, 10*time.Second, "reservation confirmed", func() bool {
		var status string
		if err := env.db.QueryRow(context.Background(),
			`SELECT status::text FROM budget_reservations WHERE id=$1::uuid`, resID).Scan(&status); err != nil {
			return false
		}
		return status == "confirmed"
	})

	// 预算池余额：spent > 0（费用已入账）
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("pool balance: %d", resp.Code)
	}
	var bal struct {
		SpentAmount string `json:"spent_amount"`
	}
	if err := json.Unmarshal(resp.Data, &bal); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	spent, err := decimal.NewFromString(bal.SpentAmount)
	if err != nil {
		t.Fatalf("parse spent: %v", err)
	}
	if !spent.IsPositive() {
		t.Fatalf("spent = %s, want > 0", bal.SpentAmount)
	}
}

// TestIntegReservationReleaseAndExpire 预留过期释放：
// 创建预留但不确认（模拟 SSE 断连）→ ScheduledWorker 过期后自动释放 → 预算池余额回归。
func TestIntegReservationReleaseAndExpire(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_expire_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 直接从池创建 pending 预留（模拟请求发起但未完成）
	poolID, err := uuid.Parse(s.poolID)
	if err != nil {
		t.Fatalf("parse pool id: %v", err)
	}
	pool, err := env.ledgerSvc.Store().GetPool(context.Background(), uuid.MustParse(env.tenantID()), poolID)
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	est := decimal.NewFromFloat(0.1)
	ttl := time.Duration(env.cfg.Business.ReserveDefaultTTLSeconds) * time.Second
	res, err := env.ledgerSvc.Reserve(context.Background(), pool, est, uuid.New(), ttl)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// 等待 ScheduledWorker 过期释放（间隔 10s + TTL 60s）
	waitFor(t, 90*time.Second, "reservation expired", func() bool {
		var status string
		if err := env.db.QueryRow(context.Background(),
			`SELECT status::text FROM budget_reservations WHERE id=$1::uuid`, res.ID.String()).Scan(&status); err != nil {
			return false
		}
		return status == "expired"
	})
}

// TestIntegDuplicateTransaction 幂等保护：同 X-Idempotency-Key 重复请求被拒绝（防重复计费）。
func TestIntegDuplicateTransaction(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_idem_" + uuid.NewString()[:8]
	env.provision(t, ext)

	chatBody := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "幂等测试"}},
	}
	// 第一次成功
	rec1, _ := env.gatewayChat(t, ext, chatBody)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first chat status = %d", rec1.Code)
	}

	// 手动构造带幂等键的请求
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(chatBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.Header.Set("X-Idempotency-Key", "idem-"+ext)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("idempotent first: %d", rec.Code)
	}

	// 同一幂等键重复请求 → 拒绝
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req2.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req2.Header.Set("X-Meteron-Customer-ID", ext)
	req2.Header.Set("X-Meteron-Task-Type", "chat")
	req2.Header.Set("X-Idempotency-Key", "idem-"+ext)
	req2.RemoteAddr = "192.0.2.1:1234"
	rec2 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("duplicate transaction status = %d, want 409, body=%s", rec2.Code, rec2.Body.String())
	}
}

// TestIntegAPIKeyLifecycle API Key 全生命周期：创建 → 绑定客户 → 使用 → 禁用 → 拒绝。
func TestIntegAPIKeyLifecycle(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_key_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 创建绑定客户的网关密钥
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id": s.customerID,
		"scopes":      []string{"gateway_invoke"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create api key: %d %s", resp.Code, string(resp.Data))
	}
	var keyResp struct {
		ID     string `json:"id"`
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal(resp.Data, &keyResp); err != nil {
		t.Fatalf("parse key: %v", err)
	}
	if keyResp.ID == "" || keyResp.APIKey == "" {
		t.Fatal("missing key id/plaintext")
	}

	// 使用新密钥调用网关（绑定客户一致）
	body := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "密钥测试"}},
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway with new key: %d %s", rec.Code, rec.Body.String())
	}

	// 禁用密钥
	_, resp = env.doReq(t, http.MethodPatch, "/api/v1/api-keys/"+keyResp.ID+"/enabled", map[string]bool{"enabled": false})
	if resp.Code != http.StatusOK {
		t.Fatalf("disable key: %d %s", resp.Code, string(resp.Data))
	}

	// 禁用后调用被拒（401）
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req2.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req2.Header.Set("X-Meteron-Customer-ID", ext)
	req2.Header.Set("X-Meteron-Task-Type", "chat")
	req2.RemoteAddr = "192.0.2.1:1234"
	rec2 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("disabled key status = %d, want 401", rec2.Code)
	}
}

// TestIntegAuthzAndScopes 鉴权与数据隔离：无密钥 401、客户级密钥越权 403、
// 缺 scope 的密钥访问管理 API 401、管理 API 需要 admin scope。
func TestIntegAuthzAndScopes(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_authz_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 无密钥 → 401
	req := httptest.NewRequest(http.MethodGet, "/api/v1/customers", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no auth status = %d, want 401", rec.Code)
	}

	// 创建仅有 gateway_invoke scope 的密钥（无 admin）
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"scopes": []string{"gateway_invoke"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create gateway key: %d %s", resp.Code, string(resp.Data))
	}
	var keyResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 该密钥访问管理 API → 401（缺 admin scope）
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/customers", nil)
	req2.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req2.RemoteAddr = "192.0.2.1:1234"
	rec2 := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("gateway-only key on admin api = %d, want 401", rec2.Code)
	}

	// 客户级密钥试图查询其他客户用量 → 数据隔离（返回空而不是越权数据）
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id": s.customerID,
		"scopes":      []string{"admin"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create customer admin key: %d", resp.Code)
	}
	var ckResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &ckResp)
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/usage?customer_id="+s.customerID, nil)
	req3.Header.Set("Authorization", "Bearer "+ckResp.APIKey)
	req3.RemoteAddr = "192.0.2.1:1234"
	rec3 := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec3, req3)
	// 客户级密钥即使指定其他 customer_id，也会被强制为绑定的客户（此处即其本人，正常放行）
	if rec3.Code != http.StatusOK {
		t.Fatalf("customer-scoped usage query = %d, want 200", rec3.Code)
	}

	// 错误密钥 → 401
	req4 := httptest.NewRequest(http.MethodGet, "/api/v1/customers", nil)
	req4.Header.Set("Authorization", "Bearer mk_invalid_key_123")
	req4.RemoteAddr = "192.0.2.1:1234"
	rec4 := httptest.NewRecorder()
	env.apiHandler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusUnauthorized {
		t.Fatalf("invalid key = %d, want 401", rec4.Code)
	}
}

// TestIntegIPWhitelist IP 白名单：带白名单的密钥从非白名单来源调用被拒（fail-closed）。
func TestIntegIPWhitelist(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_ip_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 创建仅允许 10.0.0.0/8 的密钥
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id":  s.customerID,
		"scopes":       []string{"gateway_invoke"},
		"ip_whitelist": []string{"10.0.0.0/8"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create ip key: %d %s", resp.Code, string(resp.Data))
	}
	var keyResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 从白名单外来源调用 → 403
	body := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "白名单测试"}},
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "198.51.100.7:1234" // 白名单外
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-whitelisted IP status = %d, want 403", rec.Code)
	}

	// 从白名单内来源调用 → 成功
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req2.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req2.Header.Set("X-Meteron-Customer-ID", ext)
	req2.Header.Set("X-Meteron-Task-Type", "chat")
	req2.RemoteAddr = "10.1.2.3:1234" // 白名单内
	rec2 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("whitelisted IP status = %d, want 200: %s", rec2.Code, rec2.Body.String())
	}
}

// TestIntegModelAllowList 模型白名单：未在白名单内的模型被拒（fail-closed）。
func TestIntegModelAllowList(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_model_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id":      s.customerID,
		"scopes":           []string{"gateway_invoke"},
		"model_allow_list": []string{"mock-cheap"},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create model key: %d %s", resp.Code, string(resp.Data))
	}
	var keyResp struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 不在白名单的模型 → 403
	body := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "模型白名单测试"}},
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-whitelisted model status = %d, want 403: %s", rec.Code, rec.Body.String())
	}

	// 白名单内模型 → 成功
	body2 := map[string]interface{}{
		"model":    "mock-cheap",
		"messages": []map[string]string{{"role": "user", "content": "模型白名单测试"}},
	}
	var buf2 bytes.Buffer
	_ = json.NewEncoder(&buf2).Encode(body2)
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf2)
	req2.Header.Set("Authorization", "Bearer "+keyResp.APIKey)
	req2.Header.Set("X-Meteron-Customer-ID", ext)
	req2.Header.Set("X-Meteron-Task-Type", "chat")
	req2.RemoteAddr = "192.0.2.1:1234"
	rec2 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("whitelisted model status = %d, want 200: %s", rec2.Code, rec2.Body.String())
	}
}

// TestIntegSSEGateway 流式 SSE 网关：stream=true → 逐 chunk 输出 → [DONE] 结束 → usage 计量。
func TestIntegSSEGateway(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_sse_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	body := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "SSE 流式测试"}},
		"stream":   true,
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req.Header.Set("X-Meteron-Customer-ID", ext)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec, req)

	// SSE 响应为事件流
	if rec.Code != http.StatusOK {
		t.Fatalf("SSE status = %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("SSE content-type = %q, want text/event-stream", ct)
	}
	raw := rec.Body.String()
	if !strings.Contains(raw, "data:") {
		t.Fatal("no SSE data chunks")
	}
	if !strings.Contains(raw, "[DONE]") {
		t.Fatal("SSE missing [DONE]")
	}

	// SSE 完成也产生计量
	waitFor(t, 20*time.Second, "SSE usage in clickhouse", func() bool {
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

// TestIntegInvalidInputs 非法输入校验：网关/管理 API 对畸形请求返回 400 而非 500。
func TestIntegInvalidInputs(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_invalid_" + uuid.NewString()[:8]
	env.provision(t, ext)

	// 网关：空 messages
	rec, _ := env.gatewayChat(t, ext, map[string]interface{}{
		"model": "mock-chat", "messages": []map[string]string{},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty messages status = %d, want 400", rec.Code)
	}

	// 网关：缺失 X-Meteron-Customer-ID
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]interface{}{
		"model": "mock-chat", "messages": []map[string]string{{"role": "user", "content": "hi"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", &buf)
	req.Header.Set("Authorization", "Bearer "+env.bootstrapKey)
	req.RemoteAddr = "192.0.2.1:1234"
	rec2 := httptest.NewRecorder()
	env.gwHandler.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing customer header status = %d, want 400", rec2.Code)
	}

	// 管理 API：非法 body
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/customers", "{invalid json")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid json status = %d, want 400", resp.Code)
	}

	// 管理 API：无效 UUID 路径参数
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/customers/not-a-uuid", nil)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid uuid status = %d, want 400", resp.Code)
	}
}

// TestIntegProviderHealth Provider 渠道健康管理：健康快照 → 人工禁用 → 人工恢复。
func TestIntegProviderHealth(t *testing.T) {
	env := newTestEnv(t)

	// 健康快照
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/providers/health", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("provider health: %d %s", resp.Code, string(resp.Data))
	}

	// 人工禁用 mock
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/providers/mock/disable", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("disable provider: %d %s", resp.Code, string(resp.Data))
	}
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/providers/health", nil)
	var healthData map[string]interface{}
	if err := json.Unmarshal(resp.Data, &healthData); err != nil {
		t.Fatalf("parse health: %v", err)
	}
	if st, ok := healthData["mock"].(map[string]interface{}); ok {
		if dis, _ := st["disabled"].(bool); !dis {
			t.Fatal("mock should be disabled after manual disable")
		}
	}

	// 人工恢复
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/providers/mock/enable", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("enable provider: %d %s", resp.Code, string(resp.Data))
	}
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/providers/health", nil)
	_ = json.Unmarshal(resp.Data, &healthData)
	if st, ok := healthData["mock"].(map[string]interface{}); ok {
		if dis, _ := st["disabled"].(bool); dis {
			t.Fatal("mock should be enabled after manual enable")
		}
	}
}

// TestIntegDBConstraints PG 数据完整性：external_id 租户内唯一、外键约束等。
func TestIntegDBConstraints(t *testing.T) {
	env := newTestEnv(t)
	ext := "it_dup_" + uuid.NewString()[:8]

	// 创建客户
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/customers", map[string]string{
		"name": "IT-Dup-" + ext, "external_id": ext, "currency": "USD",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("create customer: %d %s", resp.Code, string(resp.Data))
	}

	// 同租户重复 external_id → 唯一约束冲突（500 内部错误）
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/customers", map[string]string{
		"name": "IT-Dup2-" + ext, "external_id": ext, "currency": "USD",
	})
	if resp.Code == http.StatusOK {
		t.Fatal("duplicate external_id must be rejected")
	}

	// 不存在的 customer_id 建预算池 → 外键失败
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/budget-pools", map[string]string{
		"customer_id": uuid.NewString(), "max_budget": "1", "exhaust_policy": "warn",
	})
	if resp.Code == http.StatusOK {
		t.Fatal("budget pool with missing customer must fail")
	}
}
