package health

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestTracker 注入固定时钟的 Tracker（窗口 5m / 阈值 0.5 / 最小样本 4）。
func newTestTracker() (*Tracker, *time.Time) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	tr := NewTracker(5*time.Minute, 0.5, 4, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr.now = func() time.Time { return now }
	return tr, &now
}

func TestTrackerNotDisabledBelowMinSamples(t *testing.T) {
	tr, _ := newTestTracker()
	// 3 个样本全失败，但未达 minSamples=4 => 不自动禁用（防小样本误杀）
	for i := 0; i < 3; i++ {
		tr.Observe("p", false, 10*time.Millisecond)
	}
	if tr.IsDisabled("p") {
		t.Fatal("must not auto-disable below min samples")
	}
}

func TestTrackerAutoDisableOnHighErrorRate(t *testing.T) {
	tr, _ := newTestTracker()
	// 4 个样本，3 失败（75% > 50% 阈值）=> 自动禁用
	for i := 0; i < 3; i++ {
		tr.Observe("p", false, 10*time.Millisecond)
	}
	tr.Observe("p", true, 10*time.Millisecond)
	if !tr.IsDisabled("p") {
		t.Fatal("should auto-disable at 75% error rate with 4 samples")
	}
	h := tr.SnapshotOf("p")
	if !h.AutoDisabled || !h.Disabled {
		t.Fatalf("snapshot should report auto-disabled: %+v", h)
	}
	if h.ErrorRate != 0.75 {
		t.Fatalf("error rate = %v, want 0.75", h.ErrorRate)
	}
}

func TestTrackerNoDisableAtThresholdBoundary(t *testing.T) {
	tr, _ := newTestTracker()
	// 4 个样本 2 失败 = 恰好 50%（>= 阈值）=> 禁用（语义为“达到阈值即禁用”）
	for i := 0; i < 2; i++ {
		tr.Observe("p", false, time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		tr.Observe("p", true, time.Millisecond)
	}
	if !tr.IsDisabled("p") {
		t.Fatal("error rate == threshold should disable (>= semantics)")
	}
}

func TestTrackerLowErrorRateNeverDisables(t *testing.T) {
	tr, _ := newTestTracker()
	// 10 个样本 1 失败（10% < 50%）=> 不禁用
	for i := 0; i < 9; i++ {
		tr.Observe("p", true, time.Millisecond)
	}
	tr.Observe("p", false, time.Millisecond)
	if tr.IsDisabled("p") {
		t.Fatal("10% error rate must not disable")
	}
}

func TestTrackerAutoDisableLatchesUntilManualEnable(t *testing.T) {
	tr, _ := newTestTracker()
	// 触发自动禁用
	for i := 0; i < 4; i++ {
		tr.Observe("p", false, time.Millisecond)
	}
	if !tr.IsDisabled("p") {
		t.Fatal("precondition: auto-disabled")
	}
	// 后续大量成功样本：仍保持禁用（仅升不降，避免抖动渠道反复抽打上游）
	for i := 0; i < 100; i++ {
		tr.Observe("p", true, time.Millisecond)
	}
	if !tr.IsDisabled("p") {
		t.Fatal("auto-disable must latch until manual Enable")
	}
	// 人工恢复：清除自动禁用
	tr.Enable("p")
	if tr.IsDisabled("p") {
		t.Fatal("manual Enable should clear auto-disable")
	}
	// 恢复后高错误率可再次触发
	for i := 0; i < 4; i++ {
		tr.Observe("p", false, time.Millisecond)
	}
	if !tr.IsDisabled("p") {
		t.Fatal("should re-disable after recovery if error rate stays high")
	}
}

func TestTrackerManualDisableAndEnable(t *testing.T) {
	tr, _ := newTestTracker()
	tr.Disable("p")
	if !tr.IsDisabled("p") {
		t.Fatal("manual disable should take effect")
	}
	h := tr.SnapshotOf("p")
	if h.AutoDisabled {
		t.Fatal("manual disable must not be reported as auto")
	}
	tr.Enable("p")
	if tr.IsDisabled("p") {
		t.Fatal("manual enable should re-enable")
	}
}

// 无流量渠道（从未 Observe/探测）被人工禁用后必须仍出现在 Snapshot 列表中：
// 只遍历 samples 会让运维刚禁用的冷渠道"消失"，无法查看/恢复其状态。
func TestSnapshotIncludesDisabledColdProvider(t *testing.T) {
	tr, _ := newTestTracker()
	// "warm" 有流量；"cold" 只有禁用状态、无任何样本
	tr.Observe("warm", true, time.Millisecond)
	tr.Disable("cold")

	got := tr.Snapshot()
	byName := map[string]ProviderHealth{}
	for _, h := range got {
		byName[h.Provider] = h
	}
	cold, ok := byName["cold"]
	if !ok {
		t.Fatalf("manually disabled cold provider missing from snapshot: %+v", got)
	}
	if !cold.Disabled || cold.AutoDisabled {
		t.Fatalf("cold provider state = %+v, want disabled=true auto=false", cold)
	}
	if _, ok := byName["warm"]; !ok {
		t.Fatal("warm provider must still be present")
	}
	// 恢复后仍在列表（状态为启用）
	tr.Enable("cold")
	found := false
	for _, h := range tr.Snapshot() {
		if h.Provider == "cold" {
			found = true
			if h.Disabled {
				t.Fatal("cold provider must be enabled after Enable")
			}
		}
	}
	if !found {
		t.Fatal("cold provider must remain in snapshot after Enable")
	}
}

func TestTrackerWindowSliding(t *testing.T) {
	tr, now := newTestTracker()
	// t0：4 次失败 → 自动禁用；随后人工恢复
	for i := 0; i < 4; i++ {
		tr.Observe("p", false, time.Millisecond)
	}
	tr.Enable("p")

	// 时间前进超过窗口（5m+1s），旧失败样本全部过期
	*now = now.Add(5*time.Minute + time.Second)
	h := tr.SnapshotOf("p")
	if h.Total != 0 || h.Errors != 0 || h.ErrorRate != 0 {
		t.Fatalf("expired samples should be pruned: %+v", h)
	}
	// 窗口滑动后不再禁用（新样本 1 成功）
	tr.Observe("p", true, time.Millisecond)
	if tr.IsDisabled("p") {
		t.Fatal("fresh window must not carry old failures")
	}
}

func TestTrackerWindowKeepsRecentSamples(t *testing.T) {
	tr, now := newTestTracker()
	*now = now.Add(-2 * time.Minute) // 窗口内（5m 窗口）
	tr.Observe("p", true, time.Millisecond)
	*now = now.Add(-2 * time.Minute) // 4 分钟前，仍在窗口内
	tr.Observe("p", true, time.Millisecond)
	*now = now.Add(3 * time.Minute) // 现在：样本年龄 2m/4m 均在 5m 窗口内
	h := tr.SnapshotOf("p")
	if h.Total != 2 {
		t.Fatalf("total = %d, want 2 (both samples in window)", h.Total)
	}
}

func TestTrackerSnapshotAggregates(t *testing.T) {
	tr, _ := newTestTracker()
	tr.Observe("p", true, 10*time.Millisecond)
	tr.Observe("p", true, 30*time.Millisecond)
	h := tr.SnapshotOf("p")
	if h.Total != 2 || h.Errors != 0 {
		t.Fatalf("aggregate wrong: %+v", h)
	}
	if h.AvgLatencyMs != 20 {
		t.Fatalf("avg latency = %v ms, want 20", h.AvgLatencyMs)
	}
}

func TestTrackerUnknownProviderSnapshot(t *testing.T) {
	tr, _ := newTestTracker()
	h := tr.SnapshotOf("nope")
	if h.Provider != "nope" || h.Total != 0 || h.Disabled {
		t.Fatalf("unknown provider snapshot should be zero-value: %+v", h)
	}
	if tr.IsDisabled("nope") {
		t.Fatal("unknown provider must not be disabled")
	}
}

func TestTrackerProbesRecordedSeparately(t *testing.T) {
	tr, _ := newTestTracker()
	tr.ObserveProbe("p", true, time.Millisecond)
	h := tr.SnapshotOf("p")
	if h.Total != 1 {
		t.Fatalf("probe sample missing: %+v", h)
	}
	if h.LastProbeAt.IsZero() {
		t.Fatal("last probe time should be recorded")
	}
}

// ---- Prober ----

func TestProberHTTPSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized) // 任意 HTTP 响应都算网络可达
	}))
	defer srv.Close()

	tr, _ := newTestTracker()
	p := NewProber(tr, []ProbeTarget{{Name: "p", BaseURL: srv.URL}}, time.Minute, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.probeOnce()

	h := tr.SnapshotOf("p")
	if h.Total != 1 || h.Errors != 0 {
		t.Fatalf("reachable endpoint must be success: %+v", h)
	}
}

func TestProberNetworkFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // 立即关闭 => 连接失败

	tr, _ := newTestTracker()
	p := NewProber(tr, []ProbeTarget{{Name: "p", BaseURL: url}}, time.Minute, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.probeOnce()

	h := tr.SnapshotOf("p")
	if h.Total != 1 || h.Errors != 1 {
		t.Fatalf("unreachable endpoint must be failure: %+v", h)
	}
}

func TestProberEmptyBaseURLIsSuccess(t *testing.T) {
	tr, _ := newTestTracker()
	p := NewProber(tr, []ProbeTarget{{Name: "mock", BaseURL: ""}}, time.Minute, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.probeOnce()
	h := tr.SnapshotOf("mock")
	if h.Total != 1 || h.Errors != 0 {
		t.Fatalf("local/mock channel should probe success: %+v", h)
	}
}

func TestProberRunStopsOnContextCancel(t *testing.T) {
	tr, _ := newTestTracker()
	p := NewProber(tr, []ProbeTarget{{Name: "mock", BaseURL: ""}}, time.Hour, time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// 已取消的 context：Run 应立即返回（不 panic、不阻塞）
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return on cancelled context")
	}
}
