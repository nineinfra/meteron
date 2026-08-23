package gateway

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/meteron/meteron/internal/provider"
)

// usageSniffer 透传原始字节流，同时嗅探 OpenAI 格式 SSE 中的 usage chunk。
// 适用于所有适配器（anthropic 已在适配器内翻译为 OpenAI chunk 格式）。
type usageSniffer struct {
	r       io.Reader
	pending []byte // 不完整行缓冲
	usage   *provider.Usage
}

func newUsageSniffer(r io.Reader) *usageSniffer {
	return &usageSniffer{r: r}
}

func (s *usageSniffer) Usage() *provider.Usage { return s.usage }

func (s *usageSniffer) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.consume(p[:n])
	}
	return n, err
}

func (s *usageSniffer) consume(chunk []byte) {
	data := append(s.pending, chunk...)
	for {
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			break
		}
		line := data[:idx]
		data = data[idx+1:]
		s.processLine(line)
	}
	// 保留不完整行（拷贝，避免持有过大的底层数组）
	s.pending = append(s.pending[:0], data...)
}

func (s *usageSniffer) processLine(line []byte) {
	line = bytes.TrimRight(line, "\r")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	var parsed struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return
	}
	if parsed.Usage != nil {
		u := &provider.Usage{
			InputTokens:  uint64(parsed.Usage.PromptTokens),
			OutputTokens: uint64(parsed.Usage.CompletionTokens),
		}
		if parsed.Usage.PromptTokensDetails != nil {
			u.CachedInputTokens = uint64(parsed.Usage.PromptTokensDetails.CachedTokens)
		}
		s.usage = u
	}
}
