package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// OpenAICompatible OpenAI 兼容协议适配器（openai / zhipu GLM / qwen DashScope compatible-mode 等）。
type OpenAICompatible struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewOpenAICompatible(name, baseURL, apiKey string, timeout time.Duration) *OpenAICompatible {
	return &OpenAICompatible{
		name:    name,
		baseURL: baseURL,
		apiKey:  apiKey,
		client:  &http.Client{Timeout: timeout},
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	resp, err := a.client.Do(httpReq)
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
