package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/meteron/meteron/internal/config"
)

// ---- buildProfile（auto 路由请求画像：复杂度评估信号）----

func newProfileHandler(cpt int) *Handler {
	return &Handler{cfg: &config.Config{Business: config.Business{EstimateCharsPerToken: cpt}}}
}

func profileReq(quality string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if quality != "" {
		r.Header.Set(headerQuality, quality)
	}
	return r
}

func TestBuildProfileQualityHeader(t *testing.T) {
	h := newProfileHandler(4)
	for q, want := range map[string]string{"low": "low", "HIGH": "high", " Medium ": "medium"} {
		req := profileReq(q)
		var chat chatRequest
		if got := h.buildProfile(req, &chat).Quality; got != want {
			t.Errorf("quality %q: got %q, want %q（须小写归一 + 去空白）", q, got, want)
		}
	}
	// 无 header
	if got := h.buildProfile(profileReq(""), &chatRequest{}).Quality; got != "" {
		t.Fatalf("empty header: quality = %q, want empty", got)
	}
}

func TestBuildProfileInputTokens(t *testing.T) {
	h := newProfileHandler(4)
	chat := &chatRequest{Messages: []chatMessage{
		{Role: "user", Content: json.RawMessage(`"hello world, this is a test"`)},  // 27 chars
		{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"abcde"}]`)}, // 5 chars
	}}
	// 32 chars / 4 + 1 = 9（输入估算不含 max_tokens）
	if got := h.buildProfile(profileReq(""), chat).InputTokens; got != 9 {
		t.Fatalf("input tokens = %d, want 9", got)
	}
}

func TestBuildProfileImagesAndTools(t *testing.T) {
	h := newProfileHandler(4)
	chat := &chatRequest{
		Messages: []chatMessage{
			{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"看图"},{"type":"image_url","image_url":{"url":"data:..."}}]`)},
		},
		Tools: json.RawMessage(`[{"type":"function","function":{"name":"get_weather"}}]`),
	}
	p := h.buildProfile(profileReq(""), chat)
	if !p.HasImages {
		t.Fatal("image_url content part must set HasImages")
	}
	if !p.HasTools {
		t.Fatal("non-empty tools must set HasTools")
	}

	// tools 为 null / 空数组 / 缺失：不视为工具调用
	for _, tools := range []string{"null", "[]", ""} {
		chat.Tools = json.RawMessage(tools)
		if h.buildProfile(profileReq(""), chat).HasTools {
			t.Fatalf("tools=%q must not set HasTools", tools)
		}
	}
}

// ---- contentHasImage ----

func TestContentHasImage(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    bool
	}{
		{"纯文本字符串", `"hello"`, false},
		{"纯文本 parts", `[{"type":"text","text":"hi"}]`, false},
		{"含图片", `[{"type":"text","text":"hi"},{"type":"image_url","image_url":{}}]`, true},
		{"仅图片", `[{"type":"image_url"}]`, true},
		{"空数组", `[]`, false},
		{"非法 JSON", `{invalid`, false},
	}
	for _, c := range cases {
		if got := contentHasImage(json.RawMessage(c.content)); got != c.want {
			t.Errorf("%s: contentHasImage = %v, want %v", c.name, got, c.want)
		}
	}
}

// ---- auto 模式判定（handler 5.5 分支依据）----

func TestAutoModelDetection(t *testing.T) {
	cases := map[string]bool{
		"auto": true, "AUTO": true, "Auto": true,
		"gpt-4o": false, "automix": false, "": false,
	}
	for model, want := range cases {
		if got := strings.EqualFold(model, modelAuto); got != want {
			t.Errorf("model %q: isAuto = %v, want %v", model, got, want)
		}
	}
}
