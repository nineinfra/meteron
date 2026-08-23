package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// OpenAICompatible OpenAI 兼容协议适配器（openai / zhipu GLM / qwen DashScope compatible-mode 等）。
type OpenAICompatible struct {
	name    string
	baseURL string
	apiKey  string
	timeout time.Duration
	client  *http.Client
}

func NewOpenAICompatible(name, baseURL, apiKey string, timeout time.Duration) *OpenAICompatible {
	// 注意：timeout 不能设为 client.Timeout —— 它覆盖整个请求周期（含响应体
	// 读取），SSE 流式响应会在该时刻被硬截断（本地思考模型推理可达数分钟）。
	// 这里只约束"建连+模型加载+首字节"（ResponseHeaderTimeout），流式时长
	// 由调用方 context 控制（客户端断开即取消上游请求）。
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	return &OpenAICompatible{
		name:    name,
		baseURL: baseURL,
		apiKey:  apiKey,
		timeout: timeout,
		client:  &http.Client{Transport: transport},
	}
}

func (a *OpenAICompatible) Name() string { return a.name }

type openAIUsage struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	PromptTokensDetails *struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details,omitempty"`
}

func (a *OpenAICompatible) Chat(ctx context.Context, req *ChatRequest) (*ChatResult, error) {
	if !req.Stream {
		// 非流式：整体限时，防止上游挂起（流式不设整体上限，见构造函数注释）
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}
	body := req.Body
	if req.Stream {
		// 注入 stream_options.include_usage，确保流末尾返回 usage chunk
		var m map[string]interface{}
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, fmt.Errorf("parse request body: %w", err)
		}
		so, _ := m["stream_options"].(map[string]interface{})
		if so == nil {
			so = map[string]interface{}{}
		}
		so["include_usage"] = true
		m["stream_options"] = so
		nb, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		body = nb
	}

	// 网络错误 / 5xx 自动重试（本地 Ollama 排队超时、推理中断等瞬时故障）
	resp, err := postJSONWithRetry(ctx, a.client, a.baseURL+"/chat/completions", body, func(h http.Header) {
		if a.apiKey != "" {
			h.Set("Authorization", "Bearer "+a.apiKey)
		}
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
		// SSE 透传（本身即 OpenAI 格式），usage 由网关嗅探解析
		return &ChatResult{
			StatusCode:  resp.StatusCode,
			ContentType: resp.Header.Get("Content-Type"),
			Stream:      resp.Body,
		}, nil
	}

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Usage *openAIUsage `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("parse openai response: %w", err)
	}
	result := &ChatResult{StatusCode: resp.StatusCode, ContentType: "application/json", Body: respBody}
	if parsed.Usage != nil {
		u := &Usage{
			InputTokens:  uint64(parsed.Usage.PromptTokens),
			OutputTokens: uint64(parsed.Usage.CompletionTokens),
		}
		if parsed.Usage.PromptTokensDetails != nil {
			u.CachedInputTokens = uint64(parsed.Usage.PromptTokensDetails.CachedTokens)
		}
		result.Usage = u
	}
	return result, nil
}

var _ Adapter = (*OpenAICompatible)(nil)
