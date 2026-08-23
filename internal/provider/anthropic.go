package provider

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Anthropic Claude Messages API 适配器：请求/响应双向转换为 OpenAI 格式。
type Anthropic struct {
	name    string
	baseURL string
	apiKey  string
	timeout time.Duration
	client  *http.Client
}

func NewAnthropic(name, baseURL, apiKey string, timeout time.Duration) *Anthropic {
	// timeout 仅约束到响应头就绪（同 OpenAICompatible：client.Timeout 会硬截断
	// SSE 流式响应体，本地/长思考模型推理可远超该时长）。
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	return &Anthropic{
		name:    name,
		baseURL: strings.TrimSuffix(baseURL, "/"),
		apiKey:  apiKey,
		timeout: timeout,
		client:  &http.Client{Transport: transport},
	}
}

func (a *Anthropic) Name() string { return a.name }

// openAIMessage OpenAI 请求消息（content 兼容 string 与分段数组）。
type openAIMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

func (m openAIMessage) text() string {
	var s string
	if err := json.Unmarshal(m.Content, &s); err == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(m.Content, &parts); err == nil {
		var sb strings.Builder
		for _, p := range parts {
			if p.Type == "text" || p.Type == "" {
				sb.WriteString(p.Text)
			}
		}
		return sb.String()
	}
	return ""
}

type anthropicRequest struct {
	Model     string        `json:"model"`
	MaxTokens int           `json:"max_tokens"`
	System    string        `json:"system,omitempty"`
	Stream    bool          `json:"stream,omitempty"`
	Messages  []anthropicMsg `json:"messages"`
}

type anthropicMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (a *Anthropic) Chat(ctx context.Context, req *ChatRequest) (*ChatResult, error) {
	if !req.Stream {
		// 非流式：整体限时，防止上游挂起（流式不设整体上限，见构造函数注释）
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}
	// OpenAI 请求 → Anthropic 请求
	var openaiReq struct {
		Model       string          `json:"model"`
		Messages    []openAIMessage `json:"messages"`
		MaxTokens   *int            `json:"max_tokens"`
		Temperature *float64        `json:"temperature"`
		Stream      bool            `json:"stream"`
	}
	if err := json.Unmarshal(req.Body, &openaiReq); err != nil {
		return nil, fmt.Errorf("parse request body: %w", err)
	}
	ar := anthropicRequest{
		Model:     req.Model,
		MaxTokens: 4096,
		Stream:    req.Stream,
	}
	if openaiReq.MaxTokens != nil && *openaiReq.MaxTokens > 0 {
		ar.MaxTokens = *openaiReq.MaxTokens
	}
	var msgs []anthropicMsg
	for _, m := range openaiReq.Messages {
		switch m.Role {
		case "system":
			if ar.System == "" {
				ar.System = m.text()
			} else {
				ar.System += "\n" + m.text()
			}
		case "user", "assistant":
			msgs = append(msgs, anthropicMsg{Role: m.Role, Content: m.text()})
		}
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("empty messages")
	}
	ar.Messages = msgs
	body, err := json.Marshal(ar)
	if err != nil {
		return nil, err
	}

	// 网络错误 / 5xx 自动重试（瞬时故障）
	resp, err := postJSONWithRetry(ctx, a.client, a.baseURL+"/v1/messages", body, func(h http.Header) {
		h.Set("x-api-key", a.apiKey)
		h.Set("anthropic-version", "2023-06-01")
	})
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		resp.Body.Close()
		return &ChatResult{StatusCode: resp.StatusCode, Body: errBody}, nil
	}

	if req.Stream {
		// 流式：Anthropic SSE → OpenAI SSE 翻译管道
		return &ChatResult{
			StatusCode:  resp.StatusCode,
			ContentType: "text/event-stream",
			Stream:      newAnthropicSSETranslator(resp.Body, req.Model),
		}, nil
	}

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	var arResp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      *struct {
			InputTokens         int64 `json:"input_tokens"`
			OutputTokens        int64 `json:"output_tokens"`
			CacheReadInputTokens int64 `json:"cache_read_input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &arResp); err != nil {
		return nil, fmt.Errorf("parse anthropic response: %w", err)
	}
	var text strings.Builder
	for _, c := range arResp.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}
	finish := "stop"
	if arResp.StopReason != "" {
		finish = arResp.StopReason
	}
	out := map[string]interface{}{
		"id":      "chatcmpl-" + arResp.ID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   arResp.Model,
		"choices": []map[string]interface{}{{
			"index":         0,
			"message":       map[string]interface{}{"role": "assistant", "content": text.String()},
			"finish_reason": finish,
		}},
	}
	usage := &Usage{}
	if arResp.Usage != nil {
		usage.InputTokens = uint64(arResp.Usage.InputTokens)
		usage.OutputTokens = uint64(arResp.Usage.OutputTokens)
		usage.CachedInputTokens = uint64(arResp.Usage.CacheReadInputTokens)
		out["usage"] = map[string]interface{}{
			"prompt_tokens":     usage.InputTokens,
			"completion_tokens": usage.OutputTokens,
			"prompt_tokens_details": map[string]interface{}{
				"cached_tokens": usage.CachedInputTokens,
			},
		}
	}
	outBody, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return &ChatResult{
		StatusCode:  resp.StatusCode,
		ContentType: "application/json",
		Body:        outBody,
		Usage:       usage,
	}, nil
}

// anthropicSSETranslator 将 Anthropic SSE 流实时翻译为 OpenAI chunk 格式。
type anthropicSSETranslator struct {
	reader io.ReadCloser // 上游
	pr     *io.PipeReader
	pw     *io.PipeWriter
}

func newAnthropicSSETranslator(upstream io.ReadCloser, model string) *anthropicSSETranslator {
	t := &anthropicSSETranslator{reader: upstream}
	t.pr, t.pw = io.Pipe()
	go t.run(model)
	return t
}

func (t *anthropicSSETranslator) run(model string) {
	defer t.reader.Close()
	defer t.pw.Close()

	inputTokens := int64(0)
	outputTokens := int64(0)
	cacheRead := int64(0)
	sawMessageStart := false

	writeChunk := func(obj map[string]interface{}) error {
		obj["id"] = "chatcmpl-anthropic"
		obj["object"] = "chat.completion.chunk"
		obj["created"] = time.Now().Unix()
		obj["model"] = model
		b, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		if _, err := t.pw.Write(append(append([]byte("data: "), b...), '\n', '\n')); err != nil {
			return err
		}
		return nil
	}

	br := bufio.NewReaderSize(t.reader, 1024*1024)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var ev struct {
			Type    string `json:"type"`
			Message *struct {
				Usage *struct {
					InputTokens          int64 `json:"input_tokens"`
					OutputTokens         int64 `json:"output_tokens"`
					CacheReadInputTokens int64 `json:"cache_read_input_tokens"`
				} `json:"usage"`
			} `json:"message"`
			Delta *struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"delta"`
			Usage *struct {
				OutputTokens int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "message_start":
			sawMessageStart = true
			if ev.Message != nil && ev.Message.Usage != nil {
				inputTokens = ev.Message.Usage.InputTokens
				cacheRead = ev.Message.Usage.CacheReadInputTokens
			}
			if err := writeChunk(map[string]interface{}{
				"choices": []map[string]interface{}{{"index": 0, "delta": map[string]string{"role": "assistant"}, "finish_reason": nil}},
			}); err != nil {
				return
			}
		case "content_block_delta":
			if ev.Delta != nil && ev.Delta.Type == "text_delta" {
				if err := writeChunk(map[string]interface{}{
					"choices": []map[string]interface{}{{"index": 0, "delta": map[string]string{"content": ev.Delta.Text}, "finish_reason": nil}},
				}); err != nil {
					return
				}
			}
		case "message_delta":
			if ev.Usage != nil {
				outputTokens = ev.Usage.OutputTokens
			}
		case "message_stop":
			if err := writeChunk(map[string]interface{}{
				"choices": []map[string]interface{}{{"index": 0, "delta": map[string]string{}, "finish_reason": "stop"}},
				"usage": map[string]interface{}{
					"prompt_tokens":          inputTokens,
					"completion_tokens":      outputTokens,
					"prompt_tokens_details": map[string]interface{}{"cached_tokens": cacheRead},
				},
			}); err != nil {
				return
			}
			if _, err := t.pw.Write([]byte("data: [DONE]\n\n")); err != nil {
				return
			}
			return
		}
	}
	// 上游意外结束（未见 message_stop）：直接终止，网关按"流中断"处理
	if sawMessageStart {
		_, _ = t.pw.Write([]byte("data: [DONE]\n\n"))
	}
}

func (t *anthropicSSETranslator) Read(p []byte) (int, error) { return t.pr.Read(p) }
func (t *anthropicSSETranslator) Close() error {
	_ = t.pr.Close()
	return t.reader.Close()
}

var _ Adapter = (*Anthropic)(nil)
