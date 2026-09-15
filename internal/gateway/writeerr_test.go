package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/meteron/meteron/internal/common"
)

type gwErrBody struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Detail string `json:"detail"`
	} `json:"data"`
}

func decodeGWErrBody(t *testing.T, rec *httptest.ResponseRecorder) gwErrBody {
	t.Helper()
	var b gwErrBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode body: %v (raw=%s)", err, rec.Body.String())
	}
	return b
}

// 4xx 保留 detail（如限流提示需要告知上限值）。
func TestWriteErrKeepsDetailForClientErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErr(rec, common.ErrRateLimited.Wrap(errors.New("customer rpm limit 60/min")))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	b := decodeGWErrBody(t, rec)
	if !strings.Contains(b.Data.Detail, "60/min") {
		t.Fatalf("4xx detail should be preserved, got %q", b.Data.Detail)
	}
}

// 5xx 脱敏：PG/CH 错误文本不得进响应。
func TestWriteErrHidesDetailForServerErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErr(rec, common.ErrInternal.Wrap(errors.New(`clickhouse: insert into meter_events: dial tcp 10.1.2.3:9000: connect: refused`)))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	b := decodeGWErrBody(t, rec)
	if b.Data.Detail != "internal error" {
		t.Fatalf("5xx detail must be redacted, got %q", b.Data.Detail)
	}
	if strings.Contains(rec.Body.String(), "10.1.2.3") {
		t.Fatalf("internal address leaked to client: %s", rec.Body.String())
	}
}
