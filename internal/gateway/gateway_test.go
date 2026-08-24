package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/ratelimit"
)

// ---- clientIP（可信代理语义：对端命中 trustedProxies 才解析 XFF，右起第一个非可信 IP）----

var testTrusted = []string{"127.0.0.1", "::1", "10.0.0.0/8"}

func TestClientIP(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		xff        string
		trusted    []string
		want       string
	}{
		// 直连（无代理）：始终取 RemoteAddr
		{"RemoteAddr 提取 host", "192.0.2.10:52341", "", testTrusted, "192.0.2.10"},
		{"无端口 RemoteAddr 原样", "unix", "", testTrusted, "unix"},
		{"IPv6 RemoteAddr", "[2001:db8::1]:443", "", nil, "2001:db8::1"},

		// 对端为可信代理：解析 XFF（右起第一个非可信 IP）
		{"可信代理 + 单段 XFF", "127.0.0.1:1000", "203.0.113.5", testTrusted, "203.0.113.5"},
		{"可信代理 + 多段 XFF 右起取真实客户端", "127.0.0.1:1000", "spoofed, 203.0.113.5", testTrusted, "203.0.113.5"},
		{"可信 CIDR 代理 + 双层可信代理链", "10.1.2.3:1000", "203.0.113.5, 10.9.9.9", testTrusted, "203.0.113.5"},
		{"XFF 带空白容忍", "127.0.0.1:1000", " 203.0.113.5 ", testTrusted, "203.0.113.5"},
		{"可信代理但 XFF 全为可信代理：取对端", "10.1.2.3:1000", "10.0.0.9", testTrusted, "10.1.2.3"},
		{"可信代理无 XFF：取对端", "127.0.0.1:1000", "", testTrusted, "127.0.0.1"},

		// 安全关键：对端不可信时伪造 XFF 必须被忽略（IP 白名单绕过防护）
		{"直连伪造 XFF 被忽略", "203.0.113.99:1000", "127.0.0.1", testTrusted, "203.0.113.99"},
		{"直连伪造多段 XFF 被忽略", "198.51.100.7:1000", "10.0.0.1, 127.0.0.1", testTrusted, "198.51.100.7"},
		{"无可信代理配置伪造 XFF 被忽略", "203.0.113.99:1000", "127.0.0.1", nil, "203.0.113.99"},

		// 边界：XFF 混入非法段（跳过，继续向左）
		{"XFF 非法段跳过", "127.0.0.1:1000", "garbage, 203.0.113.5", testTrusted, "203.0.113.5"},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		r.RemoteAddr = c.remoteAddr
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if got := clientIP(r, c.trusted); got != c.want {
			t.Errorf("%s: clientIP = %q, want %q", c.name, got, c.want)
		}
	}
}

// ---- rewriteBodyModel（P0：路由/降级切换模型后必须重写 body.model）----

func TestRewriteBodyModel(t *testing.T) {
	body := []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"custom_field":{"a":1}}`)
	out, err := rewriteBodyModel(body, "gpt-4o-mini")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["model"] != "gpt-4o-mini" {
		t.Fatalf("model = %v, want gpt-4o-mini", m["model"])
	}
	// 其余字段原样保留（含未知字段）
	if m["max_tokens"] != float64(100) {
		t.Fatalf("max_tokens must be preserved: %v", m["max_tokens"])
	}
	if _, ok := m["custom_field"]; !ok {
		t.Fatal("unknown fields must be preserved")
	}
	if _, ok := m["messages"]; !ok {
		t.Fatal("messages must be preserved")
	}
}

func TestRewriteBodyModelInvalidJSON(t *testing.T) {
	if _, err := rewriteBodyModel([]byte(`{invalid`), "m"); err == nil {
		t.Fatal("invalid json must error")
	}
}

// ---- customerAllowed（客户级密钥绑定校验）----

func TestCustomerAllowed(t *testing.T) {
	bound := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	other := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	cases := []struct {
		name              string
		keyBinding, asked uuid.UUID
		want              bool
	}{
		{"平台级密钥不限制", uuid.Nil, other, true},
		{"绑定客户一致放行", bound, bound, true},
		{"绑定客户不一致拒绝", bound, other, false}, // 安全关键：防越权计费
	}
	for _, c := range cases {
		if got := customerAllowed(c.keyBinding, c.asked); got != c.want {
			t.Errorf("%s: customerAllowed = %v, want %v", c.name, got, c.want)
		}
	}
}

// ---- validateChatRequest（入口静态校验）----

func TestValidateChatRequestMaxTokens(t *testing.T) {
	mk := func(mt int) *chatRequest {
		return &chatRequest{
			Messages:  []chatMessage{{Role: "user", Content: json.RawMessage(`"hi"`)}},
			MaxTokens: &mt,
		}
	}
	if err := validateChatRequest(mk(1000)); err != nil {
		t.Fatalf("normal max_tokens must pass: %v", err)
	}
	if err := validateChatRequest(mk(-1)); err == nil {
		t.Fatal("negative max_tokens must be rejected")
	}
	// 安全关键：天文数字 max_tokens 会让 estimateTokens 的 int64 转换溢出为负，
	// 绕过 TPM 限流并污染滑动窗口——必须在入口拒绝
	if err := validateChatRequest(mk(1 << 62)); err == nil {
		t.Fatal("huge max_tokens must be rejected (TPM bypass via overflow)")
	}
	// 恰好上限放行
	if err := validateChatRequest(mk(maxTokensLimit)); err != nil {
		t.Fatalf("max_tokens == limit must pass: %v", err)
	}
	// 无 max_tokens（nil）与空 messages
	noMT := &chatRequest{Messages: []chatMessage{{Role: "user", Content: json.RawMessage(`"hi"`)}}}
	if err := validateChatRequest(noMT); err != nil {
		t.Fatalf("nil max_tokens must pass: %v", err)
	}
	if err := validateChatRequest(&chatRequest{}); err == nil {
		t.Fatal("empty messages must be rejected")
	}
}

// ---- 限流 key 构造 ----

func TestRateLimitKeys(t *testing.T) {
	tid, cid := uuid.MustParse("11111111-1111-1111-1111-111111111111"), uuid.MustParse("22222222-2222-2222-2222-222222222222")
	got := customerRateKey(tid, cid, "rpm")
	want := "rl:c:11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222:rpm"
	if got != want {
		t.Fatalf("customerRateKey = %q, want %q", got, want)
	}
	if got := providerConcKey("ollama"); got != "rl:p:ollama:conc" {
		t.Fatalf("providerConcKey = %q", got)
	}
	// 维度不同的 key 必须隔离（rpm/tpm/conc 不串）
	if customerRateKey(tid, cid, "rpm") == customerRateKey(tid, cid, "tpm") {
		t.Fatal("rpm/tpm keys must differ")
	}
}

// ---- applyCustomerRateLimits ----

// newLimitTestHandler 构造仅含限流依赖的最小 Handler（同包测试可直取私有字段）。
func newLimitTestHandler(rl config.RateLimit) (*Handler, *ratelimit.MemoryLimiter) {
	return &Handler{
		cfg:     &config.Config{RateLimit: rl},
		limiter: ratelimit.NewMemoryLimiter(),
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, nil
}

func testChatReq(maxTokens int) *chatRequest {
	mt := maxTokens
	return &chatRequest{
		Model:    "m",
		Messages: []chatMessage{{Role: "user", Content: json.RawMessage(`"hello world, this is a test"`)}},
		MaxTokens: &mt,
	}
}

func TestApplyCustomerRateLimitsUnlimited(t *testing.T) {
	h, _ := newLimitTestHandler(config.RateLimit{}) // 全 0=不限
	w := httptest.NewRecorder()
	release, ok := h.applyCustomerRateLimits(w, context.Background(), uuid.New(), uuid.New(), testChatReq(100))
	if !ok || release != nil {
		t.Fatalf("zero limits must pass with nil release: ok=%v release=%v", ok, release != nil)
	}
}

func TestApplyCustomerRateLimitsNilLimiter(t *testing.T) {
	h := &Handler{cfg: &config.Config{RateLimit: config.RateLimit{CustomerRPM: 1}}}
	h.limiter = nil // 未启用限流
	w := httptest.NewRecorder()
	release, ok := h.applyCustomerRateLimits(w, context.Background(), uuid.New(), uuid.New(), testChatReq(100))
	if !ok {
		t.Fatal("nil limiter must pass through")
	}
	if release != nil {
		t.Fatal("nil limiter must not return release")
	}
}

func TestApplyCustomerRateLimitsRPM(t *testing.T) {
	h, _ := newLimitTestHandler(config.RateLimit{CustomerRPM: 2})
	ctx := context.Background()
	tid, cid := uuid.New(), uuid.New()
	// 前两次放行
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		if _, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(100)); !ok {
			t.Fatalf("request %d should pass", i+1)
		}
	}
	// 第三次 429 + 错误码 RATE_LIMITED
	w := httptest.NewRecorder()
	if _, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(100)); ok {
		t.Fatal("3rd request must be rate limited")
	}
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("http status = %d, want 429", w.Code)
	}
	var body struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != 1014 {
		t.Fatalf("biz code = %d, want 1014 (RATE_LIMITED)", body.Code)
	}
	// 不同客户互不影响
	w2 := httptest.NewRecorder()
	if _, ok := h.applyCustomerRateLimits(w2, ctx, tid, uuid.New(), testChatReq(100)); !ok {
		t.Fatal("another customer must be isolated")
	}
}

func TestApplyCustomerRateLimitsTPM(t *testing.T) {
	// 请求 "hello world, this is a test"=27 字符 / 4 + 1 = 7 tokens，+max_tokens
	h, _ := newLimitTestHandler(config.RateLimit{CustomerTPM: 100})
	ctx := context.Background()
	tid, cid := uuid.New(), uuid.New()
	// max_tokens=90：cost=97 <= 100 放行
	w := httptest.NewRecorder()
	if _, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(90)); !ok {
		t.Fatal("97 tokens should pass TPM=100")
	}
	// 第二次 cost=97：累计 194 > 100 拒绝
	w = httptest.NewRecorder()
	if _, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(90)); ok {
		t.Fatal("cumulative 194 tokens must exceed TPM=100")
	}
}

func TestApplyCustomerRateLimitsConcurrency(t *testing.T) {
	h, _ := newLimitTestHandler(config.RateLimit{CustomerMaxConcurr: 1})
	ctx := context.Background()
	tid, cid := uuid.New(), uuid.New()
	// 第一次获取成功，持有许可
	w := httptest.NewRecorder()
	release, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(10))
	if !ok || release == nil {
		t.Fatal("first concurrent request should acquire")
	}
	// 第二次并发拒绝
	w = httptest.NewRecorder()
	if _, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(10)); ok {
		t.Fatal("second concurrent request must be rejected")
	}
	// 释放后可重新获取
	release()
	w = httptest.NewRecorder()
	if _, ok := h.applyCustomerRateLimits(w, ctx, tid, cid, testChatReq(10)); !ok {
		t.Fatal("acquire after release should succeed")
	}
}

// estimateTokens：字符折算 + max_tokens 上限。
func TestEstimateTokens(t *testing.T) {
	h := &Handler{cfg: &config.Config{}}
	h.cfg.Business.EstimateCharsPerToken = 4
	h.cfg.Business.EstimateDefaultMaxTokens = 1024

	// 27 字符 → 27/4+1=7；max_tokens=100 → 107
	if got := h.estimateTokens(testChatReq(100)); got != 107 {
		t.Fatalf("estimateTokens = %d, want 107", got)
	}
	// 未指定 max_tokens → 默认 1024：7+1024=1031
	if got := h.estimateTokens(&chatRequest{
		Messages: testChatReq(1).Messages,
	}); got != 1031 {
		t.Fatalf("estimateTokens default max = %d, want 1031", got)
	}
}
