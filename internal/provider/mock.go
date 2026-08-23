package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// Mock 本地模拟 Provider：无外部依赖即可跑通 demo 闭环。
// 用量确定性生成：input_tokens ≈ len(body)/4，output_tokens 默认 64（不超过 max_tokens）。
type Mock struct {
	name string
}

func NewMock(name string) *Mock { return &Mock{name: name} }

func (m *Mock) Name() string { return m.name }

func (m *Mock) Chat(ctx context.Context, req *ChatRequest) (*ChatResult, error) {
	var parsed struct {
		Messages  []openAIMessage `json:"messages"`
		MaxTokens *int            `json:"max_tokens"`
		Model     string          `json:"model"`
	}
	if err := json.Unmarshal(req.Body, &parsed); err != nil {
		return nil, fmt.Errorf("parse request body: %w", err)
	}
	inputTokens := uint64(len(req.Body) / 4)
	if inputTokens < 1 {
		inputTokens = 1
	}
	outputTokens := uint64(64)
	if parsed.MaxTokens != nil && *parsed.MaxTokens > 0 && uint64(*parsed.MaxTokens) < outputTokens {
		outputTokens = uint64(*parsed.MaxTokens)
	}
	cached := inputTokens / 4

	var promptText string
	for _, msg := range parsed.Messages {
		promptText += msg.text() + "\n"
	}
	reply := fmt.Sprintf("[mock:%s] Echo %d chars. Task model=%s. %s",
		m.name, len(promptText), req.Model, strings.Repeat("x", min(64, int(outputTokens))))

	usage := &Usage{InputTokens: inputTokens, OutputTokens: outputTokens, CachedInputTokens: cached}
	model := parsed.Model
	if model == "" {
		model = req.Model
	}

	if req.Stream {
		return &ChatResult{
			StatusCode:  200,
			ContentType: "text/event-stream",
			Stream:      newMockSSEStream(model, reply, usage),
		}, nil
	}

	respBody, _ := json.Marshal(map[string]interface{}{
		"id":      "chatcmpl-mock-" + fmt.Sprint(time.Now().UnixNano()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]interface{}{{
			"index":         0,
			"message":       map[string]interface{}{"role": "assistant", "content": reply},
			"finish_reason": "stop",
		}},
		"usage": map[string]interface{}{
			"prompt_tokens":      usage.InputTokens,
			"completion_tokens":  usage.OutputTokens,
			"prompt_tokens_details": map[string]interface{}{"cached_tokens": usage.CachedInputTokens},
		},
	})
	return &ChatResult{StatusCode: 200, ContentType: "application/json", Body: respBody, Usage: usage}, nil
}

// mockSSEStream 生成 OpenAI 格式 SSE 流（分片输出 + usage 尾包 + [DONE]）。
type mockSSEStream struct {
	chunks []string
	read   bool
}

func newMockSSEStream(model, reply string, usage *Usage) *mockSSEStream {
	mk := func(delta map[string]interface{}, finish *string, withUsage bool) string {
		obj := map[string]interface{}{
			"id":      "chatcmpl-mock",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   model,
			"choices": []map[string]interface{}{{
				"index":         0,
				"delta":         delta,
				"finish_reason": finish,
			}},
		}
		if withUsage {
			obj["usage"] = map[string]interface{}{
				"prompt_tokens":        usage.InputTokens,
				"completion_tokens":    usage.OutputTokens,
				"prompt_tokens_details": map[string]interface{}{"cached_tokens": usage.CachedInputTokens},
			}
		}
		b, _ := json.Marshal(obj)
		return "data: " + string(b) + "\n\n"
	}
	var chunks []string
	chunks = append(chunks, mk(map[string]interface{}{"role": "assistant"}, nil, false))
	// 分3片输出
	seg := (len(reply) + 2) / 3
	for i := 0; i < len(reply); i += seg {
		end := i + seg
		if end > len(reply) {
			end = len(reply)
		}
		chunks = append(chunks, mk(map[string]interface{}{"content": reply[i:end]}, nil, false))
		time.Sleep(10 * time.Millisecond) // 模拟流式延迟
	}
	stop := "stop"
	chunks = append(chunks, mk(map[string]interface{}{}, &stop, true))
	chunks = append(chunks, "data: [DONE]\n\n")
	return &mockSSEStream{chunks: chunks}
}

func (s *mockSSEStream) Read(p []byte) (int, error) {
	if s.read {
		return 0, io.EOF
	}
	var sb strings.Builder
	for _, c := range s.chunks {
		sb.WriteString(c)
	}
	data := []byte(sb.String())
	n := copy(p, data)
	s.read = true
	return n, nil
}

func (s *mockSSEStream) Close() error { return nil }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ Adapter = (*Mock)(nil)
