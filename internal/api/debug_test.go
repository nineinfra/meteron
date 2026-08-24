package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/health"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- captureWriter（调试沙盒响应捕获）----

func TestCaptureWriterDefaultsAndAppend(t *testing.T) {
	c := newCaptureWriter()
	// 未显式 WriteHeader 时首次 Write 默认 200
	if _, err := c.Write([]byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteString("world"); err != nil {
		t.Fatal(err)
	}
	if c.code != http.StatusOK {
		t.Fatalf("default code = %d, want 200", c.code)
	}
	if string(c.body) != "hello world" {
		t.Fatalf("body = %q, want %q", c.body, "hello world")
	}
}

func TestCaptureWriterExplicitHeader(t *testing.T) {
	c := newCaptureWriter()
	c.Header().Set("Content-Type", "text/event-stream")
	c.WriteHeader(http.StatusTeapot)
	_, _ = c.Write([]byte("x"))
	if c.code != http.StatusTeapot {
		t.Fatalf("code = %d, want %d", c.code, http.StatusTeapot)
	}
	if c.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatal("header must be preserved")
	}
}

// ---- debugChat 处理器 ----

// newDebugTestServer 构造仅装配 gateway 的最小 Server（同包测试）。
func newDebugTestServer(gw http.Handler) *Server {
	return &Server{
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		gateway: gw,
	}
}

func newAdminReq(method, target string, body string) *http.Request {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	ident := &rbac.Identity{TenantID: uuid.New(), Scopes: []string{rbac.ScopeAdmin}}
	return r.WithContext(withIdentity(r.Context(), ident))
}

func TestDebugChatProxiesToGateway(t *testing.T) {
	// 网关桩：记录收到的请求头并返回固定响应
	var gotAuth, gotCustomer, gotTask, gotAgent string
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustomer = r.Header.Get(headerCustomerID)
		gotTask = r.Header.Get(headerTaskType)
		gotAgent = r.Header.Get(headerAgentID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi"}}]}`))
	})
	s := newDebugTestServer(gw)

	r := newAdminReq(http.MethodPost, "/api/v1/debug/chat", `{"model":"mock-chat","messages":[]}`)
	r.Header.Set("Authorization", "Bearer mk_test_key")
	r.Header.Set(headerCustomerID, "11111111-1111-1111-1111-111111111111")
	r.Header.Set(headerTaskType, "chat")
	r.Header.Set(headerAgentID, "debug-sandbox")

	w := httptest.NewRecorder()
	s.debugChat(w, r)

	// 响应透传
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	if !strings.Contains(w.Body.String(), `"content":"hi"`) {
		t.Fatalf("gateway response not proxied: %s", w.Body.String())
	}
	// 网关收到的请求：路径 /v1/chat/completions + 元数据头透传
	if gotAuth != "Bearer mk_test_key" {
		t.Fatalf("Authorization not forwarded: %q", gotAuth)
	}
	if gotCustomer != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("customer header not forwarded: %q", gotCustomer)
	}
	if gotTask != "chat" || gotAgent != "debug-sandbox" {
		t.Fatalf("task/agent headers not forwarded: %q %q", gotTask, gotAgent)
	}
}

func TestDebugChatGatewayErrorPassesThrough(t *testing.T) {
	// 网关返回错误（如模型白名单拒绝 403）：调试接口须原样透传，便于排障
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":1021,"msg":"MODEL_NOT_ALLOWED"}`))
	})
	s := newDebugTestServer(gw)
	w := httptest.NewRecorder()
	s.debugChat(w, newAdminReq(http.MethodPost, "/api/v1/debug/chat", `{"model":"x","messages":[]}`))
	if w.Code != http.StatusForbidden {
		t.Fatalf("error status must pass through, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "MODEL_NOT_ALLOWED") {
		t.Fatalf("error body must pass through: %s", w.Body.String())
	}
}

func TestDebugChatRequiresAdminScope(t *testing.T) {
	s := newDebugTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("gateway must not be reached without admin scope")
	}))
	r := httptest.NewRequest(http.MethodPost, "/api/v1/debug/chat", strings.NewReader(`{}`))
	ident := &rbac.Identity{TenantID: uuid.New(), Scopes: []string{rbac.ScopeGateway}}
	r = r.WithContext(withIdentity(r.Context(), ident))

	w := httptest.NewRecorder()
	s.debugChat(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (missing admin scope)", w.Code)
	}
}

func TestDebugChatGatewayNotWired(t *testing.T) {
	s := newDebugTestServer(nil) // gateway 未装配
	w := httptest.NewRecorder()
	s.debugChat(w, newAdminReq(http.MethodPost, "/api/v1/debug/chat", `{}`))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
	var body struct {
		Code int `json:"code"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body.Code != common.CodeNotImplemented {
		t.Fatalf("biz code = %d, want %d", body.Code, common.CodeNotImplemented)
	}
}

// ---- Provider 渠道健康处理器 ----

func newHealthTestServer() (*Server, *health.Tracker) {
	tr := health.NewTracker(5*time.Minute, 0.5, 4,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := &Server{
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		health: tr,
	}
	return s, tr
}

func TestProviderHealthSnapshotList(t *testing.T) {
	s, tr := newHealthTestServer()
	tr.Observe("ollama", true, 20*time.Millisecond)
	tr.Observe("ollama", false, 30*time.Millisecond)

	w := httptest.NewRecorder()
	s.providerHealth(w, newAdminReq(http.MethodGet, "/api/v1/providers/health", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var env struct {
		Code int                     `json:"code"`
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Data) != 1 {
		t.Fatalf("providers = %d, want 1: %s", len(env.Data), w.Body.String())
	}
	p := env.Data[0]
	if p["provider"] != "ollama" {
		t.Fatalf("provider = %v, want ollama", p["provider"])
	}
	if p["total"] != float64(2) || p["errors"] != float64(1) {
		t.Fatalf("snapshot stats wrong: %v", p)
	}
	if p["error_rate"] != 0.5 {
		t.Fatalf("error_rate = %v, want 0.5", p["error_rate"])
	}
	if disabled, _ := p["disabled"].(bool); disabled {
		t.Fatal("50% error rate with 2 samples must not auto-disable (min_samples=4)")
	}
}

func TestProviderHealthRequiresAdminScope(t *testing.T) {
	s, _ := newHealthTestServer()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/providers/health", nil)
	ident := &rbac.Identity{TenantID: uuid.New(), Scopes: []string{rbac.ScopeGateway}}
	r = r.WithContext(withIdentity(r.Context(), ident))

	w := httptest.NewRecorder()
	s.providerHealth(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestProviderHealthNotEnabled(t *testing.T) {
	s := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))} // health=nil
	w := httptest.NewRecorder()
	s.providerHealth(w, newAdminReq(http.MethodGet, "/api/v1/providers/health", ""))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestEnableDisableProviderHandlers(t *testing.T) {
	s, tr := newHealthTestServer()

	// 人工禁用
	r := newAdminReq(http.MethodPost, "/api/v1/providers/ollama/disable", "")
	r.SetPathValue("name", "ollama")
	w := httptest.NewRecorder()
	s.disableProvider(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("disable failed: %d %s", w.Code, w.Body.String())
	}
	if !tr.IsDisabled("ollama") {
		t.Fatal("provider should be disabled")
	}
	// 禁用状态快照：disabled=true 且非自动降级
	snap := tr.SnapshotOf("ollama")
	if !snap.Disabled || snap.AutoDisabled {
		t.Fatalf("manual disable snapshot wrong: %+v", snap)
	}

	// 人工恢复
	r2 := newAdminReq(http.MethodPost, "/api/v1/providers/ollama/enable", "")
	r2.SetPathValue("name", "ollama")
	w2 := httptest.NewRecorder()
	s.enableProvider(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("enable failed: %d %s", w2.Code, w2.Body.String())
	}
	if tr.IsDisabled("ollama") {
		t.Fatal("provider should be re-enabled")
	}
}

func TestAutoDisabledProviderCanReEnable(t *testing.T) {
	s, tr := newHealthTestServer()
	// 4 样本全失败 → 自动降级
	for i := 0; i < 4; i++ {
		tr.Observe("p", false, time.Millisecond)
	}
	if !tr.IsDisabled("p") {
		t.Fatal("precondition: auto-disabled")
	}
	// 人工恢复后可继续服务
	r := newAdminReq(http.MethodPost, "/api/v1/providers/p/enable", "")
	r.SetPathValue("name", "p")
	w := httptest.NewRecorder()
	s.enableProvider(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("enable failed: %d", w.Code)
	}
	if tr.IsDisabled("p") {
		t.Fatal("manual enable must clear auto-disable")
	}
}
