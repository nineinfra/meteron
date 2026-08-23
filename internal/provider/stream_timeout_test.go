package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestOpenAICompatibleStreamNotCutByTimeout 回归测试：
// provider timeout（120s）曾通过 http.Client.Timeout 生效，它会覆盖整个
// 响应体读取——SSE 流式响应超过该时长即被硬截断，表现为"只见思考过程
// 不见正文、usage chunk 丢失"。修复后 timeout 只约束响应头就绪
// （ResponseHeaderTimeout），流式时长由调用方 context 决定。
func TestOpenAICompatibleStreamNotCutByTimeout(t *testing.T) {
	// 服务端总耗时 ~300ms（5 chunk × 60ms），超过 timeout=150ms：
	// 旧实现读到 150ms 即断流，修复后应完整收完并以 [DONE] 结束。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"%d\"}}]}\n\n", i)
			fl.Flush()
			time.Sleep(60 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer srv.Close()

	a := NewOpenAICompatible("test", srv.URL, "", 150*time.Millisecond)
	res, err := a.Chat(context.Background(), &ChatRequest{
		Model:  "m",
		Body:   []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`),
		Stream: true,
	})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	defer res.Stream.Close()

	b, err := io.ReadAll(res.Stream)
	if err != nil {
		t.Fatalf("read stream (cut by client timeout?): %v", err)
	}
	s := string(b)
	if !strings.Contains(s, `"content":"4"`) {
		t.Fatalf("late chunks lost, stream truncated: %q", s)
	}
	if !strings.Contains(s, "[DONE]") {
		t.Fatalf("stream ended without [DONE]: %q", s)
	}
}

// TestOpenAICompatibleNonStreamHonorsTimeout 非流式请求仍受 timeout 整体约束。
func TestOpenAICompatibleNonStreamHonorsTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // 超过 timeout=100ms 才响应
		fmt.Fprint(w, `{"choices":[]}`)
	}))
	defer srv.Close()

	a := NewOpenAICompatible("test", srv.URL, "", 100*time.Millisecond)
	_, err := a.Chat(context.Background(), &ChatRequest{
		Model: "m",
		Body:  []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err == nil {
		t.Fatal("non-stream request should time out")
	}
}
