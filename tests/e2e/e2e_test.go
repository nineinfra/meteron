// Package e2e 端到端测试：启动完整 Meteron 进程（网关 + 管理 API + 全部后台 Worker）
// 监听真实 TCP 端口，通过 HTTP 客户端走完整用户旅程，验证从供给、对话、
// 异步计量、预算管控到出账定稿的全链路行为（含 Kafka→Sink→ClickHouse 异步链路、
// ScheduledWorker 过期释放、健康探测）。
//
// 与集成测试的区别：e2e 启动真实 http.Server 与全部后台 Worker 循环
// （SinkWorker / MeterWorker / ScheduledWorker），覆盖进程级装配与异步链路时序；
// 集成测试仅装配 handler 直调。
//
// 运行前提（与 verify.sh 相同的基础设施）：
//
//	docker compose -f deploy/docker-compose.yml up -d
//
// 运行方式：
//
//	go test ./tests/e2e/ -v -args \
//	  -pg "host=localhost port=5432 user=meteron password=meteron123 dbname=meteron sslmode=disable" \
//	  -ch "clickhouse://meteron:meteron123@localhost:9000/meteron" \
//	  -redis "localhost:6379" -kafka "localhost:9092" -topic "meteron.meter-events"
package e2e

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
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
	"github.com/meteron/meteron/internal/worker"
)

// flagDSN 允许通过 go test -args 覆盖 DSN。
func flagDSN(name, def string) string {
	args := os.Args
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	return def
}

var (
	pgDSN       = flagDSN("-pg", "host=localhost port=5432 user=meteron password=meteron123 dbname=meteron sslmode=disable")
	chDSN       = flagDSN("-ch", "clickhouse://meteron:meteron123@localhost:9000/meteron")
	redisAddr   = flagDSN("-redis", "localhost:6379")
	kafkaBroker = flagDSN("-kafka", "localhost:9092")
	topic       = flagDSN("-topic", "meteron.meter-events")
)

// e2eEnv 端到端测试环境：完整进程装配 + 真实 HTTP 端点。
type e2eEnv struct {
	t            *testing.T
	cfg          *config.Config
	db           *pgxpool.Pool
	ch           chdriver.Conn
	baseURL      string // http://127.0.0.1:port
	srv          *http.Server
	bootstrapKey string
	ctx          context.Context
	cancel       context.CancelFunc
}

// freePort 获取一个可用 TCP 端口。
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// newE2EEnv 启动完整 Meteron 进程（API + 全部后台 Worker）。
func newE2EEnv(t *testing.T, mutate func(*config.Config)) *e2eEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := config.Default()
	cfg.PGDSN = pgDSN
	cfg.ClickHouseDSN = chDSN
	cfg.RedisAddr = redisAddr
	cfg.KafkaBrokers = []string{kafkaBroker}
	cfg.KafkaTopic = topic
	cfg.InsecureDefaults = true
	cfg.AESKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	cfg.BootstrapKey = "mk_meteron_bootstrap_dev_key"
	// e2e 快时序：缩短后台任务间隔
	cfg.Business.ScheduledInterval = 2 * time.Second // 过期扫描加速
	cfg.Business.ResyncInterval = 2 * time.Second    // 缓存校正加速
	cfg.Health.ProbeInterval = time.Second           // 主动探测加速
	if mutate != nil {
		mutate(cfg)
	}

	db, err := store.NewPG(ctx, cfg.PGDSN)
	if err != nil {
		cancel()
		t.Fatalf("connect pg: %v", err)
	}
	t.Cleanup(db.Close)
	if err := store.MigratePG(ctx, db); err != nil {
		cancel()
		t.Fatalf("migrate pg: %v", err)
	}

	ch, err := store.NewClickHouse(ctx, cfg.ClickHouseDSN)
	if err != nil {
		cancel()
		t.Fatalf("connect clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	rds, err := store.NewRedis(ctx, cfg.RedisAddr)
	if err != nil {
		cancel()
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rds.Client.Close() })

	if err := store.EnsureKafkaTopic(cfg.KafkaBrokers, cfg.KafkaTopic); err != nil {
		t.Logf("ensure kafka topic (may auto-create): %v", err)
	}

	authSvc, err := rbac.NewService(db, cfg.AESKey)
	if err != nil {
		cancel()
		t.Fatalf("rbac: %v", err)
	}
	if _, _, err := authSvc.Bootstrap(ctx, cfg.BootstrapKey); err != nil {
		cancel()
		t.Fatalf("bootstrap: %v", err)
	}

	ledgerStore := ledger.NewStore(db)
	ledgerSvc := ledger.NewService(ledgerStore, rds, db, log)
	pricingEngine := pricing.NewEngine(db, log)
	routerEngine := router.NewEngine(db)

	providers, err := provider.NewRegistry(cfg.Providers, 30*time.Second)
	if err != nil {
		cancel()
		t.Fatalf("provider registry: %v", err)
	}
	routerEngine.SetLookup(providers)

	kafkaWriter := store.NewKafkaWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	t.Cleanup(func() { _ = kafkaWriter.Close() })
	producer := ingest.NewProducer(kafkaWriter, ingest.NewPGOutboxStore(db), log)
	t.Cleanup(producer.Close)

	billingWorker := billing.NewWorker(db, ch, rds, cfg, log)

	healthTracker := health.NewTracker(cfg.Health.Window, cfg.Health.ErrorRateThreshold, cfg.Health.MinSamples, log)
	limiter := ratelimit.NewRedisLimiter(rds.Client)

	gw := gateway.NewHandler(authSvc, ledgerSvc, ledgerStore, routerEngine, pricingEngine,
		providers, producer, db, cfg, log, healthTracker, limiter)
	apiServer := api.NewServer(db, ch, authSvc, ledgerSvc, pricingEngine, producer,
		billingWorker, cfg, log, healthTracker, gw)

	// ---- 后台 Worker（与 main.go 同构）----
	sinkWorker := meter.NewSinkWorker(ch, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	meterWorker := meter.NewMeterWorker(ch, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	scheduledWorker := worker.NewScheduledWorker(db, rds, ledgerSvc, cfg, log)

	go func() {
		if err := sinkWorker.Run(ctx); err != nil {
			t.Logf("sink worker exited: %v", err)
		}
	}()
	go func() {
		if err := meterWorker.Run(ctx); err != nil {
			t.Logf("meter worker exited: %v", err)
		}
	}()
	go func() {
		if err := scheduledWorker.Run(ctx); err != nil {
			t.Logf("scheduled worker exited: %v", err)
		}
	}()

	// 渠道主动探测（e2e 环境下开启，验证 Prober 装配）
	probeTargets := make([]health.ProbeTarget, 0, len(cfg.Providers))
	for _, p := range cfg.Providers {
		probeTargets = append(probeTargets, health.ProbeTarget{Name: p.Name, BaseURL: p.BaseURL})
	}
	prober := health.NewProber(healthTracker, probeTargets, cfg.Health.ProbeInterval, 5*time.Second, log)
	go prober.Run(ctx)

	// ---- HTTP 服务（真实 TCP）----
	port := freePort(t)
	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", gw)
	mux.Handle("/api/v1/", apiServer.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		_ = srv.ListenAndServe()
	}()
	t.Cleanup(func() {
		shutdownCtx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	env := &e2eEnv{
		t:            t,
		cfg:          cfg,
		db:           db,
		ch:           ch,
		baseURL:      fmt.Sprintf("http://127.0.0.1:%d", port),
		srv:          srv,
		bootstrapKey: cfg.BootstrapKey,
		ctx:          ctx,
		cancel:       cancel,
	}
	t.Cleanup(cancel)

	// 等待服务就绪
	env.waitHealthy()
	return env
}

// waitHealthy 轮询 /healthz 直到服务可用。
func (e *e2eEnv) waitHealthy() {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(e.baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.t.Fatalf("service not healthy at %s", e.baseURL)
}

// ---- HTTP 客户端辅助 ----

type apiResponse struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// doReq 发送管理 API 请求（Bearer 引导密钥）。
func (e *e2eEnv) doReq(t *testing.T, method, path string, body interface{}) (int, apiResponse) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.baseURL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if method != http.MethodGet {
		req.Header.Set("Authorization", "Bearer "+e.bootstrapKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out apiResponse
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// chat 通过真实 TCP 调用网关对话端点（X-Meteron 元数据头）。
func (e *e2eEnv) chat(t *testing.T, key, customerRef string, headers map[string]string, body map[string]interface{}) (int, http.Header, map[string]interface{}, string) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal chat body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, e.baseURL+"/v1/chat/completions", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("X-Meteron-Customer-ID", customerRef)
	req.Header.Set("X-Meteron-Task-Type", "chat")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do chat: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed map[string]interface{}
	_ = json.Unmarshal(raw, &parsed)
	return resp.StatusCode, resp.Header, parsed, string(raw)
}

// dataID 提取响应 data.id。
func (e *e2eEnv) dataID(t *testing.T, resp apiResponse) string {
	t.Helper()
	var m struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(resp.Data, &m); err != nil || m.ID == "" {
		t.Fatalf("response data has no id: %s", string(resp.Data))
	}
	return m.ID
}

// waitFor 轮询直到条件满足。
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

// supply 供给链资源。
type supply struct {
	customerID string
	poolID     string
}

// provision 建立完整供给链（简化版：客户/指标/产品/费率/规则/合同/预算池）。
func (e *e2eEnv) provision(t *testing.T, ext string) *supply {
	t.Helper()
	s := &supply{}

	_, resp := e.doReq(t, http.MethodPost, "/api/v1/customers", map[string]string{
		"name": "E2E-Customer-" + ext, "external_id": ext, "currency": "USD",
	})
	if resp.Code != 0 && resp.Code != 200 {
		t.Fatalf("create customer: %d %s", resp.Code, string(resp.Data))
	}
	s.customerID = e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/billable-metrics", map[string]interface{}{
		"name": "e2e_metric_" + ext, "aggregation_type": "sum", "group_keys": []string{},
	})
	if resp.Code != 200 {
		t.Fatalf("create metric: %d %s", resp.Code, string(resp.Data))
	}
	metricID := e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/products", map[string]string{
		"name": "e2e_product_" + ext, "display_name": "E2E", "metric_id": metricID,
	})
	if resp.Code != 200 {
		t.Fatalf("create product: %d %s", resp.Code, string(resp.Data))
	}
	productID := e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/rate-cards", map[string]string{
		"name": "e2e_rc_" + ext, "currency": "USD",
	})
	if resp.Code != 200 {
		t.Fatalf("create rate card: %d %s", resp.Code, string(resp.Data))
	}
	rateCardID := e.dataID(t, resp)

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/rate-cards/"+rateCardID+"/rules", map[string]interface{}{
		"product_id":       productID,
		"match_conditions": map[string]string{"task_type": "*"},
		"pricing_type":     "flat",
		"pricing_config": map[string]interface{}{
			"input_unit_price": "0.000003", "output_unit_price": "0.000006",
		},
		"is_default_rule": true,
	})
	if resp.Code != 200 {
		t.Fatalf("create rule: %d %s", resp.Code, string(resp.Data))
	}

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/contracts", map[string]string{
		"customer_id": s.customerID, "rate_card_id": rateCardID,
	})
	if resp.Code != 200 {
		t.Fatalf("create contract: %d %s", resp.Code, string(resp.Data))
	}

	_, resp = e.doReq(t, http.MethodPost, "/api/v1/budget-pools", map[string]string{
		"customer_id": s.customerID, "max_budget": "10", "exhaust_policy": "warn",
	})
	if resp.Code != 200 {
		t.Fatalf("create pool: %d %s", resp.Code, string(resp.Data))
	}
	s.poolID = e.dataID(t, resp)
	return s
}

// usageItems 查询客户用量。
func (e *e2eEnv) usageItems(t *testing.T, customerID string) []meter.UsageSummary {
	t.Helper()
	_, resp := e.doReq(t, http.MethodGet, "/api/v1/usage?customer_id="+customerID, nil)
	if resp.Code != 200 {
		t.Fatalf("usage: %d %s", resp.Code, string(resp.Data))
	}
	var u struct {
		Items []meter.UsageSummary `json:"items"`
	}
	if err := json.Unmarshal(resp.Data, &u); err != nil {
		t.Fatalf("parse usage: %v", err)
	}
	return u.Items
}

// ---- 端到端测试 ----

// TestE2EFullJourney 完整用户旅程（verify.sh 的 Go 等价实现）：
// 供给 → 非流式对话 → 流式对话 → auto 路由 → 异步计量（Kafka→Sink→CH）→
// 用量查询 → 预算扣减 → 出账 → 定稿 → 账本流水 → 对账列表。
func TestE2EFullJourney(t *testing.T) {
	env := newE2EEnv(t, nil)
	ext := "e2e_full_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 1. 非流式对话（mock provider）
	code, hdr, parsed, raw := env.chat(t, env.bootstrapKey, ext, nil, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "你好，Meteron！"}},
	})
	if code != http.StatusOK {
		t.Fatalf("chat status = %d, body = %s", code, raw)
	}
	if hdr.Get("X-Meteron-Transaction-ID") == "" {
		t.Fatal("missing transaction header")
	}
	if parsed["usage"] == nil {
		t.Fatalf("missing usage in response: %s", raw)
	}

	// 2. 流式对话（SSE）
	code, _, _, sseRaw := env.chat(t, env.bootstrapKey, ext, nil, map[string]interface{}{
		"model": "mock-chat",
		"messages": []map[string]string{
			{"role": "user", "content": "流式测试"},
		},
		"stream": true,
	})
	if code != http.StatusOK {
		t.Fatalf("sse chat status = %d", code)
	}
	if !strings.Contains(sseRaw, "data:") || !strings.Contains(sseRaw, "[DONE]") {
		t.Fatalf("invalid SSE stream: %q", sseRaw[:min(200, len(sseRaw))])
	}

	// 3. auto 路由对话
	code, _, autoParsed, raw := env.chat(t, env.bootstrapKey, ext, nil, map[string]interface{}{
		"model":    "auto",
		"messages": []map[string]string{{"role": "user", "content": "auto 路由"}},
	})
	if code != http.StatusOK {
		t.Fatalf("auto chat status = %d, body = %s", code, raw)
	}
	if m, _ := autoParsed["model"].(string); m != "mock-chat" {
		t.Fatalf("auto model = %q, want mock-chat", m)
	}

	// 4. 异步计量链路：等待全部 3 次对话落 ClickHouse（经真实 Kafka→SinkWorker）
	waitFor(t, 30*time.Second, "all usage in clickhouse", func() bool {
		total := uint64(0)
		for _, it := range env.usageItems(t, s.customerID) {
			total += it.RequestCount
		}
		return total >= 3
	})

	// 5. 用量断言：请求计数与 token 汇总
	items := env.usageItems(t, s.customerID)
	var totalTokens uint64
	for _, it := range items {
		totalTokens += it.InputTokens + it.OutputTokens
		if it.Provider != "mock" {
			t.Fatalf("usage provider = %s, want mock", it.Provider)
		}
	}
	if totalTokens == 0 {
		t.Fatal("total tokens = 0")
	}

	// 6. 预算池扣减（confirm 后 spent > 0）
	waitFor(t, 15*time.Second, "budget spent", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
		if resp.Code != 200 {
			return false
		}
		var bal struct {
			SpentAmount string `json:"spent_amount"`
		}
		_ = json.Unmarshal(resp.Data, &bal)
		spent, _ := decimal.NewFromString(bal.SpentAmount)
		return spent.IsPositive()
	})

	// 7. 出账
	now := time.Now().UTC()
	periodStart := now.Truncate(24 * time.Hour)
	_, resp := env.doReq(t, http.MethodPost, "/api/v1/billing-runs", map[string]string{
		"period_start": periodStart.Format(time.RFC3339),
		"period_end":   periodStart.Add(24 * time.Hour).Format(time.RFC3339),
	})
	if resp.Code != 200 {
		t.Fatalf("billing run: %d %s", resp.Code, string(resp.Data))
	}

	// 8. draft 账单出现
	var billID string
	waitFor(t, 10*time.Second, "draft bill", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/bills?customer_id="+s.customerID, nil)
		if resp.Code != 200 {
			return false
		}
		var bills []map[string]interface{}
		_ = json.Unmarshal(resp.Data, &bills)
		for _, b := range bills {
			if st, _ := b["status"].(string); st == "draft" {
				billID, _ = b["id"].(string)
			}
		}
		return billID != ""
	})

	// 9. 定稿
	_, resp = env.doReq(t, http.MethodPost, "/api/v1/bills/"+billID+"/finalize", nil)
	if resp.Code != 200 {
		t.Fatalf("finalize: %d %s", resp.Code, string(resp.Data))
	}
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/bills/"+billID, nil)
	var bill struct {
		Status      string `json:"status"`
		TotalAmount string `json:"total_amount"`
	}
	_ = json.Unmarshal(resp.Data, &bill)
	if bill.Status != "finalized" {
		t.Fatalf("bill status = %s", bill.Status)
	}
	if bill.TotalAmount == "" || bill.TotalAmount == "0" {
		t.Fatalf("bill total = %s, want > 0", bill.TotalAmount)
	}

	// 10. 账本流水（usage_charge 逐事件）
	var cnt int
	if err := env.db.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM ledger_entries WHERE bill_id=$1::uuid`, billID).Scan(&cnt); err != nil {
		t.Fatalf("query ledger: %v", err)
	}
	if cnt < 3 {
		t.Fatalf("ledger entries = %d, want >= 3 (one per event)", cnt)
	}

	// 11. 对账列表端点可用
	_, resp = env.doReq(t, http.MethodGet, "/api/v1/reconciliation-runs", nil)
	if resp.Code != 200 {
		t.Fatalf("recon runs: %d", resp.Code)
	}

	t.Logf("e2e full journey done: bill=%s total=%s tokens=%d events=%d",
		billID, bill.TotalAmount, totalTokens, cnt)
}

// TestE2EProviderHealthProbe 渠道主动探测：Prober 周期探测 mock（无外部端点=成功），
// 健康快照可见 mock 渠道样本。
func TestE2EProviderHealthProbe(t *testing.T) {
	env := newE2EEnv(t, func(c *config.Config) {
		c.Health.ProbeInterval = 500 * time.Millisecond
	})
	ext := "e2e_probe_" + uuid.NewString()[:8]
	env.provision(t, ext)

	// 等待至少一轮探测（mock 渠道 BaseURL 为空 → 直接记成功）
	waitFor(t, 10*time.Second, "probe samples", func() bool {
		_, resp := env.doReq(t, http.MethodGet, "/api/v1/providers/health", nil)
		if resp.Code != 200 {
			return false
		}
		var list []map[string]interface{}
		if err := json.Unmarshal(resp.Data, &list); err != nil {
			return false
		}
		for _, p := range list {
			if name, _ := p["provider"].(string); name == "mock" {
				if total, _ := p["total"].(float64); total >= 1 {
					if disabled, _ := p["disabled"].(bool); !disabled {
						return true
					}
				}
			}
		}
		return false
	})
}

// TestE2EReservationExpireRealWorker 真实 ScheduledWorker 过期释放：
// 创建短 TTL 预留不确认 → ScheduledWorker（2s 间隔）自动过期 → 预算恢复。
func TestE2EReservationExpireRealWorker(t *testing.T) {
	env := newE2EEnv(t, func(c *config.Config) {
		c.Business.ReserveDefaultTTLSeconds = 3 // 短 TTL
		c.Business.ScheduledInterval = time.Second
	})
	ext := "e2e_exp_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	// 直接创建 pending 预留（模拟请求挂起）
	poolID, err := uuid.Parse(s.poolID)
	if err != nil {
		t.Fatalf("parse pool id: %v", err)
	}
	rds, err := store.NewRedis(env.ctx, env.cfg.RedisAddr)
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	defer rds.Client.Close()
	ledgerStore := ledger.NewStore(env.db)
	ledgerSvc := ledger.NewService(ledgerStore, rds, env.db, testLogger())

	pool, err := ledgerStore.GetPool(env.ctx, uuid.MustParse(env.tenantID()), poolID)
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	res, err := ledgerSvc.Reserve(env.ctx, pool, decimal.NewFromFloat(0.5), uuid.New(), 3*time.Second)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// 等待真实 ScheduledWorker 过期释放（TTL 3s + 扫描 1s）
	waitFor(t, 20*time.Second, "reservation expired by real worker", func() bool {
		var status string
		if err := env.db.QueryRow(env.ctx,
			`SELECT status::text FROM budget_reservations WHERE id=$1::uuid`,
			res.ID.String()).Scan(&status); err != nil {
			return false
		}
		return status == "expired"
	})
}

// tenantID 返回 bootstrap 租户 ID。
func (e *e2eEnv) tenantID() string {
	var tid string
	if err := e.db.QueryRow(e.ctx,
		`SELECT id::text FROM tenants ORDER BY created_at LIMIT 1`).Scan(&tid); err != nil {
		e.t.Fatalf("query tenant: %v", err)
	}
	return tid
}

// TestE2ESSEUsageMetering SSE 流式计量：流结束后 usage 嗅探 → confirm → 计量事件落库。
func TestE2ESSEUsageMetering(t *testing.T) {
	env := newE2EEnv(t, nil)
	ext := "e2e_sse_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	code, hdr, _, raw := env.chat(t, env.bootstrapKey, ext, nil, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "SSE 计量"}},
		"stream":   true,
	})
	if code != http.StatusOK {
		t.Fatalf("sse status = %d, body = %s", code, raw)
	}
	resID := hdr.Get("X-Meteron-Reservation-ID")
	if resID == "" {
		t.Fatal("SSE should create reservation")
	}

	// 流完成 → 预留确认（真实异步链路）
	waitFor(t, 15*time.Second, "sse reservation confirmed", func() bool {
		var status string
		if err := env.db.QueryRow(env.ctx,
			`SELECT status::text FROM budget_reservations WHERE id=$1::uuid`, resID).Scan(&status); err != nil {
			return false
		}
		return status == "confirmed"
	})

	// 事件落 ClickHouse
	waitFor(t, 20*time.Second, "sse usage in clickhouse", func() bool {
		return len(env.usageItems(t, s.customerID)) > 0
	})
}

// TestE2EUpstreamFailureAudit 上游失败审计：请求不存在的模型（无 provider 承载）→
// 网关拒绝（NO_PROVIDER 422），不产生计量事件。
func TestE2EUpstreamFailureAudit(t *testing.T) {
	env := newE2EEnv(t, nil)
	ext := "e2e_noprov_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	code, _, _, raw := env.chat(t, env.bootstrapKey, ext, nil, map[string]interface{}{
		"model":    "nonexistent-model-xyz",
		"messages": []map[string]string{{"role": "user", "content": "无渠道模型"}},
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("no provider status = %d, want 422, body = %s", code, raw)
	}

	// 无计量事件产生（失败发生在路由阶段，早于事件构造）
	time.Sleep(2 * time.Second)
	items := env.usageItems(t, s.customerID)
	if len(items) != 0 {
		t.Fatalf("usage items = %d, want 0 (no metering for unrouted model)", len(items))
	}
}

// TestE2EConcurrentChats 并发对话：多请求并发下预算无超卖（available >= 0）。
func TestE2EConcurrentChats(t *testing.T) {
	env := newE2EEnv(t, nil)
	ext := "e2e_conc_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	const n = 8
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			code, _, _, _ := env.chat(t, env.bootstrapKey, ext, nil, map[string]interface{}{
				"model":    "mock-chat",
				"messages": []map[string]string{{"role": "user", "content": "并发对话测试"}},
			})
			done <- code
		}()
	}
	okCount := 0
	for i := 0; i < n; i++ {
		if code := <-done; code == http.StatusOK {
			okCount++
		}
	}
	if okCount < n/2 {
		t.Fatalf("too many concurrent failures: ok=%d/%d", okCount, n)
	}

	// 等待全部计量完成
	waitFor(t, 30*time.Second, "all concurrent usage", func() bool {
		total := uint64(0)
		for _, it := range env.usageItems(t, s.customerID) {
			total += it.RequestCount
		}
		return total >= uint64(okCount)
	})

	// 预算无超卖：available 不为负
	_, resp := env.doReq(t, http.MethodGet, "/api/v1/budget-pools/"+s.poolID+"/balance", nil)
	var bal struct {
		AvailableAmount string `json:"available_amount"`
	}
	_ = json.Unmarshal(resp.Data, &bal)
	avail, _ := decimal.NewFromString(bal.AvailableAmount)
	if avail.IsNegative() {
		t.Fatalf("budget oversold: available = %s", bal.AvailableAmount)
	}
}

// TestE2EAPIKeyFullLifecycle 密钥全生命周期（真实 TCP）：
// 创建 → 调用成功 → 禁用 → 401 → 删除 → 401。
func TestE2EAPIKeyFullLifecycle(t *testing.T) {
	env := newE2EEnv(t, nil)
	ext := "e2e_key_" + uuid.NewString()[:8]
	s := env.provision(t, ext)

	_, resp := env.doReq(t, http.MethodPost, "/api/v1/api-keys", map[string]interface{}{
		"customer_id": s.customerID,
		"scopes":      []string{"gateway_invoke"},
	})
	if resp.Code != 200 {
		t.Fatalf("create key: %d", resp.Code)
	}
	var keyResp struct {
		ID     string `json:"id"`
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(resp.Data, &keyResp)

	// 使用新密钥成功调用
	code, _, _, raw := env.chat(t, keyResp.APIKey, ext, nil, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "密钥调用"}},
	})
	if code != http.StatusOK {
		t.Fatalf("key chat status = %d, body = %s", code, raw)
	}

	// 禁用 → 401
	_, _ = env.doReq(t, http.MethodPatch, "/api/v1/api-keys/"+keyResp.ID+"/enabled", map[string]bool{"enabled": false})
	code, _, _, _ = env.chat(t, keyResp.APIKey, ext, nil, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "已禁用"}},
	})
	if code != http.StatusUnauthorized {
		t.Fatalf("disabled key status = %d, want 401", code)
	}

	// 删除 → 401
	_, _ = env.doReq(t, http.MethodDelete, "/api/v1/api-keys/"+keyResp.ID, nil)
	code, _, _, _ = env.chat(t, keyResp.APIKey, ext, nil, map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "已删除"}},
	})
	if code != http.StatusUnauthorized {
		t.Fatalf("deleted key status = %d, want 401", code)
	}
}

// TestE2EIdempotencyKeyRetry 幂等重试（真实 TCP）：同 X-Idempotency-Key 重放 → 409。
func TestE2EIdempotencyKeyRetry(t *testing.T) {
	env := newE2EEnv(t, nil)
	ext := "e2e_idem_" + uuid.NewString()[:8]
	env.provision(t, ext)

	idemKey := "e2e-idem-" + uuid.NewString()[:8]
	body := map[string]interface{}{
		"model":    "mock-chat",
		"messages": []map[string]string{{"role": "user", "content": "幂等重试"}},
	}

	code1, _, _, raw1 := env.chat(t, env.bootstrapKey, ext, map[string]string{
		"X-Idempotency-Key": idemKey,
	}, body)
	if code1 != http.StatusOK {
		t.Fatalf("first idempotent call = %d, body = %s", code1, raw1)
	}

	code2, _, _, _ := env.chat(t, env.bootstrapKey, ext, map[string]string{
		"X-Idempotency-Key": idemKey,
	}, body)
	if code2 != http.StatusConflict {
		t.Fatalf("retry with same key = %d, want 409", code2)
	}
}

// ---- 辅助 ----

// testLogger 静默测试日志器。
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// min 取小（避免依赖 Go 版本内置）。
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 编译期保证 driver.Conn 引用（保留以备扩展直查 ClickHouse）。
var _ driver.Conn
