package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- 在线调试沙盒（能力5）----

// debugChat POST /api/v1/debug/chat
// 管理后台调试接口：直接复用网关完整链路（鉴权→IP/模型白名单→限流→预算预留→
// 路由→定价→转发→计量），校验路由、定价、权限配置的正确性。
//
// 实现方式：把请求原样转发给内部网关 Handler（/v1/chat/completions），
// 调用方密钥透传——用什么密钥调试就按什么密钥的真实权限执行。
// body 即 OpenAI chat/completions 请求体；支持 stream（SSE 缓冲后整段返回）。
func (s *Server) debugChat(w http.ResponseWriter, r *http.Request) {
	if requireScope(w, r, rbac.ScopeAdmin) == nil {
		return
	}
	if s.gateway == nil {
		writeErr(w, common.ErrNotImplemented.Wrap(errors.New("gateway handler not wired")))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAPIBody))
	if err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(err))
		return
	}

	// 构造内部请求：完整走网关 ServeHTTP（含计量事件，便于审计回看调试记录）
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// 透传鉴权与网关元数据头（X-Meteron-*），与真实调用方一致
	for _, h := range []string{"Authorization", headerCustomerID, headerTaskType, headerAgentID} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	// 透传来源地址与 XFF 链：网关 IP 白名单按真实来源判定。
	// 内部构造的请求 RemoteAddr 为空串，带 IP 白名单的密钥会被 fail-closed 拒绝（必现 403）。
	req.RemoteAddr = r.RemoteAddr
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		req.Header.Set("X-Forwarded-For", v)
	}

	rec := newCaptureWriter()
	s.gateway.ServeHTTP(rec, req)

	// 透传网关响应（状态码/Content-Type/错误体或模型响应体）
	w.Header().Set("Content-Type", rec.Header().Get("Content-Type"))
	w.WriteHeader(rec.code)
	_, _ = w.Write(rec.body)
}

// captureWriter 捕获网关响应（SSE 流缓冲后整段返回，调试场景可接受）。
type captureWriter struct {
	hdr   http.Header
	code  int
	body  []byte
}

func newCaptureWriter() *captureWriter      { return &captureWriter{hdr: http.Header{}} }
func (c *captureWriter) Header() http.Header { return c.hdr }
func (c *captureWriter) WriteHeader(code int) { c.code = code }
func (c *captureWriter) Write(b []byte) (int, error) {
	if c.code == 0 {
		c.code = http.StatusOK
	}
	c.body = append(c.body, b...)
	return len(b), nil
}
func (c *captureWriter) WriteString(s string) (int, error) { return c.Write([]byte(s)) }

// 网关链路所需头（与 gateway 包常量一致，避免包间耦合）
const (
	headerCustomerID = "X-Meteron-Customer-ID"
	headerTaskType   = "X-Meteron-Task-Type"
	headerAgentID    = "X-Meteron-Agent-ID"
)
