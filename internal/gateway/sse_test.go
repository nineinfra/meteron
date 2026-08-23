package gateway

import (
	"io"
	"strings"
	"testing"
)

func TestUsageSnifferParsesOpenAIChunks(t *testing.T) {
	sse := strings.Join([]string{
		`data: {"id":"c1","choices":[{"delta":{"role":"assistant"}}]}`,
		``,
		`data: {"id":"c1","choices":[{"delta":{"content":"hi"}}]}`,
		``,
		`data: {"id":"c1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":120,"completion_tokens":35,"prompt_tokens_details":{"cached_tokens":30}}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")

	s := newUsageSniffer(strings.NewReader(sse))
	buf := make([]byte, 4096)
	var n int
	var err error
	for err == nil {
		var c int
		c, err = s.Read(buf)
		n += c
	}
	if n == 0 {
		t.Fatal("sniffer should pass through bytes")
	}
	u := s.Usage()
	if u == nil {
		t.Fatal("usage not captured")
	}
	if u.InputTokens != 120 || u.OutputTokens != 35 || u.CachedInputTokens != 30 {
		t.Fatalf("usage = %+v, want {120 35 30}", u)
	}
}

func TestUsageSnifferNilWithoutUsage(t *testing.T) {
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n"
	s := newUsageSniffer(strings.NewReader(sse))
	buf := make([]byte, 1024)
	for {
		_, err := s.Read(buf)
		if err != nil {
			break
		}
	}
	if s.Usage() != nil {
		t.Fatal("no usage chunk in stream, Usage should be nil")
	}
}

func TestUsageSnifferPartialLinesAcrossReads(t *testing.T) {
	// usage chunk 被拆成两次 Read，仍应解析成功
	full := `data: {"usage":{"prompt_tokens":9,"completion_tokens":1}}` + "\n\n"
	s := newUsageSniffer(&splitReader{data: []byte(full), split: 20})
	buf := make([]byte, 16)
	for {
		_, err := s.Read(buf)
		if err != nil {
			break
		}
	}
	u := s.Usage()
	if u == nil || u.InputTokens != 9 || u.OutputTokens != 1 {
		t.Fatalf("usage = %+v, want {9 1 0}", u)
	}
}

// splitReader 每次最多返回 split 字节，模拟分片到达。
type splitReader struct {
	data  []byte
	split int
}

func (r *splitReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.split
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}
