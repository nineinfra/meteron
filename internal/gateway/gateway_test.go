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

// ---- clientIP ----

func TestClientIP(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"RemoteAddr 提取 host", "192.0.2.10:52341", "", "192.0.2.10"},
		{"无端口 RemoteAddr 原样", "unix", "", "unix"},
		{"XFF 优先", "10.0.0.1:1000", "203.0.113.5", "203.0.113.5"},
		{"XFF 多段取首段", "10.0.0.1:1000", "203.0.113.5, 70.41.3.18, 150.172.238.178", "203.0.113.5"},
		{"XFF 带空白容忍", "10.0.0.1:1000", " 203.0.113.5 ", "203.0.113.5"},
		{"IPv6 RemoteAddr", "[2001:db8::1]:443", "", "2001:db8::1"},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		r.RemoteAddr = c.remoteAddr
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if got := clientIP(r); got != c.want {
			t.Errorf("%s: clientIP = %q, want %q", c.name, got, c.want)
		}
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
