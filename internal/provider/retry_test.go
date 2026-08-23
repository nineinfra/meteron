package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetryOn5xxThenSuccess：前两次 500（瞬时故障），第三次成功 → Chat 应成功。
func TestRetryOn5xxThenSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":"boom"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":2}}`)
	}))
	defer srv.Close()

	a := NewOpenAICompatible("test", srv.URL, "", 5*time.Second)
	res, err := a.Chat(context.Background(), &ChatRequest{
		Model: "m",
		Body:  []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("chat should succeed after retries: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
}

// TestRetryExhausted：持续 500 → 3 次尝试后返回最后一次的错误响应（4xx/5xx 原样透传给调用方）。
func TestRetryExhausted(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, `{"error":"down"}`)
	}))
	defer srv.Close()

	a := NewOpenAICompatible("test", srv.URL, "", 5*time.Second)
	res, err := a.Chat(context.Background(), &ChatRequest{
		Model: "m",
		Body:  []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("exhausted 5xx should surface as ChatResult, got err: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502 preserved, got %d", res.StatusCode)
	}
}

// TestNoRetryOn4xx：400 属请求问题，不应重试。
func TestNoRetryOn4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"bad request"}`)
	}))
	defer srv.Close()

	a := NewOpenAICompatible("test", srv.URL, "", 5*time.Second)
	res, err := a.Chat(context.Background(), &ChatRequest{
		Model: "m",
		Body:  []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("4xx must not be retried, got %d calls", got)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

// TestNoRetryWhenClientGone：客户端断开（ctx 取消）后不得重试。
// 第一次尝试 500 后进入 500ms 退避；150ms 时取消 ctx → 退避中断返回，不再发起第二次。
func TestNoRetryWhenClientGone(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	a := NewOpenAICompatible("test", srv.URL, "", 5*time.Second)
	_, err := a.Chat(ctx, &ChatRequest{
		Model: "m",
		Body:  []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err == nil {
		t.Fatal("expected error after client disconnect")
	}
	// 退避期 500ms；若错误地继续重试，此刻会有第二次调用
	time.Sleep(600 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got > 1 {
		t.Fatalf("canceled context must not retry, got %d calls", got)
	}
}

// TestRetryStreamRecovers：流式请求 500 → 重试后拿到正常 SSE 流。
func TestRetryStreamRecovers(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer srv.Close()

	a := NewOpenAICompatible("test", srv.URL, "", 5*time.Second)
	res, err := a.Chat(context.Background(), &ChatRequest{
		Model:  "m",
		Body:   []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`),
		Stream: true,
	})
	if err != nil {
		t.Fatalf("stream chat should recover: %v", err)
	}
	defer res.Stream.Close()
	b, _ := io.ReadAll(res.Stream)
	if string(b) == "" || atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected recovered stream on 2nd attempt, calls=%d body=%q", atomic.LoadInt32(&calls), b)
	}
}
