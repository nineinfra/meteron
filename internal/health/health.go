// Package health Provider 渠道健康探测与自动降级（参考 new-api 渠道管理）。
// 双数据源：真实流量统计（gateway 转发结果 Observe）+ 主动探测（Prober 连通性检查）。
// 窗口内错误率超阈值自动禁用（防小样本误杀），人工 Enable 恢复；
// 路由侧通过 IsDisabled 跳过故障渠道。
package health

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/meteron/meteron/internal/metrics"
)

type sample struct {
	at      time.Time
	success bool
	latency time.Duration
}

// Tracker 渠道健康跟踪器（滑动窗口统计 + 禁用状态机）。
type Tracker struct {
	mu         sync.RWMutex
	window     time.Duration
	threshold  float64
	minSamples int64
	samples    map[string][]sample
	manual     map[string]bool // 人工禁用
	auto       map[string]bool // 自动禁用（错误率超阈值）
	lastProbe  map[string]time.Time

	log *slog.Logger
	now func() time.Time // 可注入时钟（测试）
}

func NewTracker(window time.Duration, errorRateThreshold float64, minSamples int64, log *slog.Logger) *Tracker {
	return &Tracker{
		window:     window,
		threshold:  errorRateThreshold,
		minSamples: minSamples,
		samples:    map[string][]sample{},
		manual:     map[string]bool{},
		auto:       map[string]bool{},
		lastProbe:  map[string]time.Time{},
		log:        log,
		now:        time.Now,
	}
}

// ProviderHealth 渠道健康快照（管理 API / Prometheus 同源）。
type ProviderHealth struct {
	Provider     string    `json:"provider"`
	Total        int64     `json:"total"`
	Errors       int64     `json:"errors"`
	ErrorRate    float64   `json:"error_rate"`
	AvgLatencyMs float64   `json:"avg_latency_ms"`
	Disabled     bool      `json:"disabled"`
	AutoDisabled bool      `json:"auto_disabled"`
	LastProbeAt  time.Time `json:"last_probe_at"`
}

// Observe 记录一次真实流量结果（gateway 转发后调用）。
func (t *Tracker) Observe(provider string, success bool, latency time.Duration) {
	t.observeAt(provider, success, latency, t.now())
}

func (t *Tracker) observeAt(provider string, success bool, latency time.Duration, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.appendSample(provider, sample{at: at, success: success, latency: latency}, at)
	t.evaluateLocked(provider)
	t.exportLocked(provider)
}

// ObserveProbe 记录一次主动探测结果（Prober 调用）。
func (t *Tracker) ObserveProbe(provider string, success bool, latency time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastProbe[provider] = t.now()
	t.appendSample(provider, sample{at: t.now(), success: success, latency: latency}, t.now())
	t.evaluateLocked(provider)
	t.exportLocked(provider)
}

func (t *Tracker) appendSample(provider string, s sample, now time.Time) {
	t.samples[provider] = append(t.samples[provider], s)
	// 裁剪窗口外样本（均摊 O(1)：仅移除头部过期段）
	ss := t.samples[provider]
	cutoff := now.Add(-t.window)
	i := 0
	for i < len(ss) && ss[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		t.samples[provider] = append([]sample{}, ss[i:]...)
	}
}

// evaluateLocked 自动禁用判定：样本数达到 minSamples 且错误率 >= 阈值。
// 仅"升不降"：自动禁用后不自动恢复（需人工 Enable，避免抖动渠道反复抽打上游）。
func (t *Tracker) evaluateLocked(provider string) {
	if t.auto[provider] {
		return // 已自动禁用，等人工恢复
	}
	st := t.statsLocked(provider, t.now())
	if st.total >= t.minSamples && st.errorRate >= t.threshold {
		t.auto[provider] = true
		if t.log != nil {
			t.log.Warn("provider auto-disabled by health tracker",
				"provider", provider, "error_rate", st.errorRate,
				"window", t.window.String(), "samples", st.total)
		}
	}
}

// statsLocked 按当前时间过滤窗口内样本再统计（过期样本即使尚未从内存
// 裁剪也不计入，保证 Snapshot/导出与观察语义一致）。
func (t *Tracker) statsLocked(provider string, now time.Time) (st struct {
	total     int64
	errors    int64
	errorRate float64
	avgLat    float64
}) {
	cutoff := now.Add(-t.window)
	var sumLatency time.Duration
	for _, s := range t.samples[provider] {
		if s.at.Before(cutoff) {
			continue
		}
		st.total++
		if !s.success {
			st.errors++
		}
		sumLatency += s.latency
	}
	if st.total == 0 {
		return
	}
	st.errorRate = float64(st.errors) / float64(st.total)
	st.avgLat = float64(sumLatency) / float64(st.total)
	return
}

// IsDisabled 渠道当前是否不可用（人工禁用或自动禁用）。
func (t *Tracker) IsDisabled(provider string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.manual[provider] || t.auto[provider]
}

// Disable 人工禁用渠道。
func (t *Tracker) Disable(provider string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.manual[provider] = true
	t.exportLocked(provider)
}

// Enable 人工恢复渠道：清除禁用状态并重置统计样本
// （人工确认修复后从零重新观察，旧故障样本不拖累恢复后的错误率）。
func (t *Tracker) Enable(provider string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.manual[provider] = false
	t.auto[provider] = false
	t.samples[provider] = nil
	t.exportLocked(provider)
}

// Snapshot 全部渠道健康快照。
// 遍历 samples ∪ manual ∪ auto：只看 samples 会让"从未有流量的渠道"
// （如刚配置、或探测未启动时被人工禁用）从管理列表中消失，
// 运维禁用后在列表里找不到该渠道、无法查看/恢复其状态。
func (t *Tracker) Snapshot() []ProviderHealth {
	t.mu.RLock()
	defer t.mu.RUnlock()
	seen := make(map[string]bool, len(t.samples))
	out := make([]ProviderHealth, 0, len(t.samples))
	add := func(p string) {
		if p == "" || seen[p] {
			return
		}
		seen[p] = true
		out = append(out, t.snapshotOneLocked(p))
	}
	for p := range t.samples {
		add(p)
	}
	for p := range t.manual {
		add(p)
	}
	for p := range t.auto {
		add(p)
	}
	return out
}

// SnapshotOf 单个渠道健康快照（不存在时返回零值视图）。
func (t *Tracker) SnapshotOf(provider string) ProviderHealth {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snapshotOneLocked(provider)
}

func (t *Tracker) snapshotOneLocked(provider string) ProviderHealth {
	st := t.statsLocked(provider, t.now())
	h := ProviderHealth{
		Provider:     provider,
		Total:        st.total,
		Errors:       st.errors,
		ErrorRate:    st.errorRate,
		AvgLatencyMs: st.avgLat / float64(time.Millisecond),
		Disabled:     t.manual[provider] || t.auto[provider],
		AutoDisabled: t.auto[provider],
		LastProbeAt:  t.lastProbe[provider],
	}
	return h
}

// exportLocked 同步 Prometheus 指标（Gauge Set 成本低，随状态变更即时更新）。
// provider_disabled 的三个 reason 变体全部写入（当前 reason=1、其余=0）：
// 只写当前 reason 会在标签切换（如 auto → none）后遗留旧序列恒为 1，
// 导致 provider_disabled == 1 类告警永久误报（Prometheus 不回收已导出序列）。
func (t *Tracker) exportLocked(provider string) {
	st := t.statsLocked(provider, t.now())
	metrics.ProviderHealthErrorRate.WithLabelValues(provider).Set(st.errorRate)
	metrics.ProviderHealthAvgLatencySeconds.WithLabelValues(provider).Set(st.avgLat / float64(time.Second))
	reason := "none"
	if t.auto[provider] {
		reason = "auto"
	} else if t.manual[provider] {
		reason = "manual"
	}
	for _, r := range [...]string{"none", "auto", "manual"} {
		var v float64
		if r == reason {
			v = 1
		}
		metrics.ProviderDisabled.WithLabelValues(provider, r).Set(v)
	}
}

// ---- 主动探测 ----

// ProbeTarget 探测目标（从 ProviderConfig 映射）。
type ProbeTarget struct {
	Name    string
	BaseURL string // 空 = 本地 mock 渠道，直接视为可达
}

// Prober 定时连通性探测：GET base_url，任意 HTTP 响应（含 401/404）
// 均视为"网络可达=成功"；仅网络错误/超时视为失败。不消耗上游 token。
type Prober struct {
	tracker  *Tracker
	targets  []ProbeTarget
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	log      *slog.Logger
}

func NewProber(tracker *Tracker, targets []ProbeTarget, interval, timeout time.Duration, log *slog.Logger) *Prober {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Prober{
		tracker:  tracker,
		targets:  targets,
		interval: interval,
		timeout:  timeout,
		client:   &http.Client{Timeout: timeout},
		log:      log,
	}
}

// Run 启动探测循环（interval<=0 时不探测，由调用方决定是否启动）。
func (p *Prober) Run(ctx context.Context) {
	if p.interval <= 0 {
		return
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.probeOnce()
		}
	}
}

func (p *Prober) probeOnce() {
	for _, tgt := range p.targets {
		if tgt.BaseURL == "" {
			// mock / 本地渠道无外部端点：直接记成功，保持健康数据存在
			p.tracker.ObserveProbe(tgt.Name, true, 0)
			continue
		}
		start := time.Now()
		resp, err := p.client.Get(tgt.BaseURL)
		latency := time.Since(start)
		success := err == nil
		if success {
			resp.Body.Close()
		} else if p.log != nil {
			p.log.Warn("provider probe failed", "provider", tgt.Name, "err", err)
		}
		p.tracker.ObserveProbe(tgt.Name, success, latency)
	}
}
