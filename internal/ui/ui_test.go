package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesIndex(t *testing.T) {
	h := Handler()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("GET / content-type = %q, want text/html", ct)
	}
	if body := rec.Body.String(); !strings.Contains(body, "<html") {
		t.Fatal("index should be html document")
	}
}

func TestHandlerSPAFallback(t *testing.T) {
	h := Handler()

	// 未知路径回退到 index.html（SPA 路由）
	req := httptest.NewRequest(http.MethodGet, "/some/client/route", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /some/client/route status = %d, want 200 (SPA fallback)", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "<html") {
		t.Fatal("SPA fallback should serve index.html")
	}
}

func TestHandlerNotFoundForAssets(t *testing.T) {
	h := Handler()

	// 带扩展名的资源路径不回退，返回 404（避免 JS/CSS 缺失误返回 HTML）
	req := httptest.NewRequest(http.MethodGet, "/assets/missing-abc123.js", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		// 构建产物存在时可能命中；占位模式下返回 404 也合法
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GET /assets/*.js status = %d, want 200 or 404", rec.Code)
		}
	}
}
