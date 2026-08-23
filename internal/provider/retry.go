package provider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// 上游瞬时故障重试策略：
// - 重试对象：网络/传输层错误、HTTP 5xx（如 Ollama 推理中途 500、上游网关抖动）
// - 不重试：4xx（请求本身有问题，重发必然同样失败）、客户端已断开（ctx 取消）
// - 次数：最多重试 2 次（共 3 次尝试），退避 500ms / 1500ms
// - 边界：仅覆盖"建立请求"阶段（拿到响应头为止）。流式响应一旦返回给调用方，
//   部分 chunk 已可能透传给客户端，无法安全重放。
const retryMaxAttempts = 3

var retryBackoff = []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond}

// postJSONWithRetry 发送 JSON POST，失败按上述策略重试。headerFn 在每次
// 尝试时对请求设置头（请求对象每次重建，header 不能复用旧引用）。
func postJSONWithRetry(ctx context.Context, client *http.Client, url string, body []byte, headerFn func(h http.Header)) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < retryMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("aborted before retry: %w", ctx.Err())
			case <-time.After(retryBackoff[attempt-1]):
			}
		}
		// 每次尝试重建请求（body reader 一次性）
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err // URL 非法等构造错误：重试无意义
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if headerFn != nil {
			headerFn(httpReq.Header)
		}
		resp, err := client.Do(httpReq)
		if err != nil {
			if ctx.Err() != nil {
				// 客户端已断开：立即放弃，不重试
				return nil, err
			}
			lastErr = fmt.Errorf("attempt %d: %w", attempt+1, err)
			continue
		}
		// 5xx 视为上游瞬时故障：排空并关闭连接后重试；最后一次直接返回由调用方处理
		if resp.StatusCode >= 500 && attempt < retryMaxAttempts-1 {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4*1024))
			resp.Body.Close()
			lastErr = fmt.Errorf("attempt %d: upstream %s", attempt+1, resp.Status)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("upstream failed after %d attempts: %w", retryMaxAttempts, lastErr)
}
