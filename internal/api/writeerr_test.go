package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/meteron/meteron/internal/common"
)

type errBody struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Detail string `json:"detail"`
	} `json:"data"`
}

func decodeErrBody(t *testing.T, rec *httptest.ResponseRecorder) errBody {
	t.Helper()
	var b errBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode body: %v (raw=%s)", err, rec.Body.String())
	}
	return b
}

// 4xx 是调用方可纠正的错误：保留 detail 便于自助排障。
func TestWriteErrKeepsDetailForClientErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErr(rec, common.ErrBadRequest.Wrap(errors.New("invalid expire_at")))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	b := decodeErrBody(t, rec)
	if b.Code != common.CodeBadRequest {
		t.Fatalf("code = %d, want %d", b.Code, common.CodeBadRequest)
	}
	if !strings.Contains(b.Data.Detail, "invalid expire_at") {
		t.Fatalf("4xx detail should be preserved, got %q", b.Data.Detail)
	}
}

// 5xx 属内部故障：detail 可能含 SQL/约束名/内部地址，不得回给客户端。
func TestWriteErrHidesDetailForServerErrors(t *testing.T) {
	const leaked = `pq: duplicate key value violates unique constraint "uq_bills_period_active"`
	rec := httptest.NewRecorder()
	writeErr(rec, common.ErrInternal.Wrap(errors.New(leaked)))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	b := decodeErrBody(t, rec)
	if b.Code != common.CodeInternal {
		t.Fatalf("code = %d, want %d", b.Code, common.CodeInternal)
	}
	if b.Data.Detail != "internal error" {
		t.Fatalf("5xx detail must be redacted, got %q", b.Data.Detail)
	}
	// 整个响应体都不得出现内部信息（含 msg 字段）
	if strings.Contains(rec.Body.String(), "uq_bills_period_active") {
		t.Fatalf("internal detail leaked to client: %s", rec.Body.String())
	}
}

// 上游错误（502）同样按 5xx 脱敏。
func TestWriteErrHidesDetailForUpstreamErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErr(rec, common.ErrUpstream.Wrap(errors.New("upstream 500: connection reset by 10.0.0.7:443")))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	b := decodeErrBody(t, rec)
	if b.Data.Detail != "internal error" {
		t.Fatalf("5xx detail must be redacted, got %q", b.Data.Detail)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.7") {
		t.Fatalf("upstream address leaked to client: %s", rec.Body.String())
	}
}
