package meter

import (
	"testing"
	"time"
)

// 边界语义：MeterWorker 在小时关闭 +2min 宽限后落盘（flushClosed 的
// cutoff=(now-2min) 严格小于），hourlySafeBoundary 需再留 tick + 余量。
// 因此 (now - 1h - 3min) 所在小时（含）保证已落盘，+1h 即明细覆盖起点。
func TestHourlySafeBoundary(t *testing.T) {
	cases := []struct {
		now  time.Time
		want time.Time
	}{
		// 03:00 桶于 04:02 后落盘：04:21 查询时 boundary=03:00（含），openFrom=04:00
		{time.Date(2026, 8, 23, 4, 21, 0, 0, time.UTC),
			time.Date(2026, 8, 23, 3, 0, 0, 0, time.UTC)},
		// 04:02:30：03:00 桶刚过落盘点，仍保守回退到 02:00（多查 1h 明细，正确性无损）
		{time.Date(2026, 8, 23, 4, 2, 30, 0, time.UTC),
			time.Date(2026, 8, 23, 2, 0, 0, 0, time.UTC)},
		// 整点附近
		{time.Date(2026, 8, 23, 5, 0, 0, 0, time.UTC),
			time.Date(2026, 8, 23, 3, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		if got := hourlySafeBoundary(c.now); !got.Equal(c.want) {
			t.Fatalf("hourlySafeBoundary(%v) = %v, want %v", c.now, got, c.want)
		}
	}
}

// 合并逻辑：相同维度（task|model|provider）的两段结果应累加而非覆盖。
func TestMergeUsageAdd(t *testing.T) {
	merged := map[string]*UsageSummary{}
	mergeUsageAdd(merged, UsageSummary{TaskType: "chat", Model: "gpt", Provider: "openai",
		InputTokens: 100, OutputTokens: 50, CachedTokens: 10, RequestCount: 2})
	mergeUsageAdd(merged, UsageSummary{TaskType: "chat", Model: "gpt", Provider: "openai",
		InputTokens: 30, OutputTokens: 20, CachedTokens: 5, RequestCount: 1})
	mergeUsageAdd(merged, UsageSummary{TaskType: "chat", Model: "local", Provider: "local",
		InputTokens: 7, RequestCount: 1})

	if len(merged) != 2 {
		t.Fatalf("merged groups = %d, want 2", len(merged))
	}
	g := merged["chat|gpt|openai"]
	if g.InputTokens != 130 || g.OutputTokens != 70 || g.CachedTokens != 15 || g.RequestCount != 3 {
		t.Fatalf("merged gpt row = %+v, want accumulated values", *g)
	}
	if merged["chat|local|local"].InputTokens != 7 {
		t.Fatal("distinct dimension row missing")
	}
}
