// Package provider ProviderAdapter 抽象与各厂商实现（openai兼容 / anthropic / mock）。
// 所有适配器对外统一输出 OpenAI 格式响应/SSE，usage 归一化为
// input_tokens / output_tokens / cached_input_tokens。
package provider

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/meteron/meteron/internal/config"
)

// Usage 归一化用量。
type Usage struct {
	InputTokens       uint64 `json:"input_tokens"`
	OutputTokens      uint64 `json:"output_tokens"`
	CachedInputTokens uint64 `json:"cached_input_tokens"`
}

// ChatRequest 归一化请求（Body 为 OpenAI chat/completions 格式 JSON）。
type ChatRequest struct {
	Model  string
	Body   []byte
	Stream bool
}

// ChatResult 上游响应。
type ChatResult struct {
	StatusCode  int
	ContentType string
	Body        []byte          // 非流式：完整 OpenAI 格式响应体
	Stream      io.ReadCloser   // 流式：OpenAI 格式 SSE 流
	Usage       *Usage          // 非流式：归一化用量
}

// Adapter Provider 适配器接口。
type Adapter interface {
	Name() string
	Chat(ctx context.Context, req *ChatRequest) (*ChatResult, error)
}

// Registry Provider 注册表。
type Registry struct {
	adapters   map[string]Adapter
	modelIndex map[string]string // model -> provider name
}

func NewRegistry(cfgs []config.ProviderConfig, timeout time.Duration) (*Registry, error) {
	r := &Registry{adapters: map[string]Adapter{}, modelIndex: map[string]string{}}
	for _, c := range cfgs {
		var a Adapter
		switch c.Type {
		case "openai", "openai-compatible", "local":
			if c.BaseURL == "" {
				return nil, fmt.Errorf("provider %s: %s 类型必须配置 base_url", c.Name, c.Type)
			}
			// local：本地部署模型（Ollama / vLLM / LM Studio / llama.cpp-server 等
			// OpenAI 兼容端点），api_key 可留空（适配器仅在非空时携带 Authorization 头）
			a = NewOpenAICompatible(c.Name, c.BaseURL, c.APIKey, timeout)
		case "anthropic":
			if c.BaseURL == "" {
				return nil, fmt.Errorf("provider %s: anthropic 类型必须配置 base_url", c.Name)
			}
			a = NewAnthropic(c.Name, c.BaseURL, c.APIKey, timeout)
		case "mock":
			a = NewMock(c.Name)
		default:
			return nil, fmt.Errorf("provider %s: 未知类型 %q", c.Name, c.Type)
		}
		r.adapters[c.Name] = a
		for _, m := range c.Models {
			r.modelIndex[m] = c.Name
		}
	}
	return r, nil
}

// Get 按名称获取适配器。
func (r *Registry) Get(name string) (Adapter, bool) {
	a, ok := r.adapters[name]
	return a, ok
}

// ProviderForModel 按模型名反查 provider（无路由规则时使用）。
func (r *Registry) ProviderForModel(model string) (string, bool) {
	p, ok := r.modelIndex[model]
	return p, ok
}
