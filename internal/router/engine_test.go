package router

import (
	"testing"
)

// ---- evalTier：复杂度评估 → 成本分级 ----

func TestEvalTierExplicitQuality(t *testing.T) {
	var a autoPolicy
	cases := []struct {
		quality string
		want    string
	}{
		{"low", "economy"},
		{"medium", "standard"},
		{"high", "premium"},
		{"", ""}, // 空：启发式（短输入 economy）
		{"LOW", ""}, // 大小写不敏感由调用方归一化；此处原样不命中 → 启发式
	}
	for _, c := range cases {
		got := a.evalTier(RequestProfile{Quality: c.quality})
		want := c.want
		if want == "" {
			want = "economy" // 无 quality + 短输入
		}
		if got != want {
			t.Errorf("evalTier(quality=%q) = %q, want %q", c.quality, got, want)
		}
	}
}

func TestEvalTierHeuristicInputTokens(t *testing.T) {
	var a autoPolicy
	cases := []struct {
		inputTokens uint64
		want        string
	}{
		{100, "economy"},
		{1999, "economy"},
		{2000, "standard"},
		{7999, "standard"},
		{8000, "premium"},
	}
	for _, c := range cases {
		if got := a.evalTier(RequestProfile{InputTokens: c.inputTokens}); got != c.want {
			t.Errorf("evalTier(input=%d) = %q, want %q", c.inputTokens, got, c.want)
		}
	}
}

func TestEvalTierCustomThresholds(t *testing.T) {
	a := autoPolicy{StandardMinInputTokens: 500, PremiumMinInputTokens: 2000}
	if got := a.evalTier(RequestProfile{InputTokens: 499}); got != "economy" {
		t.Errorf("499 tokens with custom threshold = %q, want economy", got)
	}
	if got := a.evalTier(RequestProfile{InputTokens: 500}); got != "standard" {
		t.Errorf("500 tokens with custom threshold = %q, want standard", got)
	}
	if got := a.evalTier(RequestProfile{InputTokens: 2000}); got != "premium" {
		t.Errorf("2000 tokens with custom threshold = %q, want premium", got)
	}
}

func TestEvalTierImagesAndToolsForcePremium(t *testing.T) {
	var a autoPolicy
	// 多模态/工具调用对模型能力要求高：即使输入很短也走 premium
	if got := a.evalTier(RequestProfile{InputTokens: 10, HasImages: true}); got != "premium" {
		t.Errorf("images = %q, want premium", got)
	}
	if got := a.evalTier(RequestProfile{InputTokens: 10, HasTools: true}); got != "premium" {
		t.Errorf("tools = %q, want premium", got)
	}
	// 显式 quality 优先于启发式：low + 图片仍 economy（调用方明确要求低成本）
	if got := a.evalTier(RequestProfile{Quality: "low", HasImages: true}); got != "economy" {
		t.Errorf("quality=low + images = %q, want economy（显式优先）", got)
	}
}

// ---- tierIndex ----

func TestTierIndex(t *testing.T) {
	if tierIndex("economy") != 0 || tierIndex("standard") != 1 || tierIndex("premium") != 2 {
		t.Fatal("tier order must be economy<standard<premium")
	}
	if tierIndex("unknown") != -1 {
		t.Fatal("unknown tier must be -1")
	}
}

// ---- autoTargetFor：分级目标解析 ----

type stubLookup struct {
	tiers   map[string][]string
	models  map[string]string
}

func (s *stubLookup) ProvidersByTier(tier string) []string { return s.tiers[tier] }
func (s *stubLookup) DefaultModelForProvider(name string) (string, bool) {
	m, ok := s.models[name]
	return m, ok
}

func TestAutoTargetForExplicitTiers(t *testing.T) {
	// tiers 显式配置优先于 cost_tier 反查
	e := &Engine{}
	auto := autoPolicy{Tiers: map[string]autoTier{
		"economy": {Provider: "local", Model: "qwen2.5:32b"},
	}}
	res, ok := e.autoTargetFor("economy", auto, nil)
	if !ok || res.Provider != "local" || res.Model != "qwen2.5:32b" {
		t.Fatalf("explicit tier: ok=%v res=%+v", ok, res)
	}
}

func TestAutoTargetForExplicitTierModelFallback(t *testing.T) {
	// tiers 配置省略 model：取渠道默认模型
	e := &Engine{lookup: &stubLookup{
		models: map[string]string{"deepseek": "deepseek-chat"},
	}}
	auto := autoPolicy{Tiers: map[string]autoTier{
		"standard": {Provider: "deepseek"},
	}}
	res, ok := e.autoTargetFor("standard", auto, nil)
	if !ok || res.Provider != "deepseek" || res.Model != "deepseek-chat" {
		t.Fatalf("model fallback: ok=%v res=%+v", ok, res)
	}
	// 省略 model 且 lookup 无该渠道：目标不可用
	e2 := &Engine{lookup: &stubLookup{models: map[string]string{}}}
	if _, ok := e2.autoTargetFor("standard", auto, nil); ok {
		t.Fatal("provider without models must be unavailable")
	}
}

func TestAutoTargetForTierLookup(t *testing.T) {
	// 未显式配置：按 provider.cost_tier 反查（配置序第一个）
	e := &Engine{lookup: &stubLookup{
		tiers:  map[string][]string{"premium": {"openai", "anthropic"}},
		models: map[string]string{"openai": "gpt-4o", "anthropic": "claude-3-5"},
	}}
	res, ok := e.autoTargetFor("premium", autoPolicy{}, nil)
	if !ok || res.Provider != "openai" || res.Model != "gpt-4o" {
		t.Fatalf("tier lookup: ok=%v res=%+v", ok, res)
	}
	// 无该分级渠道
	if _, ok := e.autoTargetFor("economy", autoPolicy{}, nil); ok {
		t.Fatal("missing tier must be unavailable")
	}
	// 无 lookup（未注入）
	e2 := &Engine{}
	if _, ok := e2.autoTargetFor("premium", autoPolicy{}, nil); ok {
		t.Fatal("nil lookup must be unavailable")
	}
}

// ---- ResolveAuto 核心路径（无规则 → 默认阈值 + lookup 反查）----
// 注：完整 ResolveAuto 含 DB 读规则，此处通过 autoTargetFor + evalTier 组合覆盖决策矩阵；
// DB 交互与 Resolve 同层（项目无 pgx mock 基建，遵循现状不测）。

func TestAutoDecisionMatrix(t *testing.T) {
	// 三级渠道齐全：economy=本地，standard=国内，premium=国外（成本策略典型布局）
	e := &Engine{lookup: &stubLookup{
		tiers: map[string][]string{
			"economy":  {"ollama"},
			"standard": {"deepseek"},
			"premium":  {"openai"},
		},
		models: map[string]string{
			"ollama":   "qwen3.5:9b",
			"deepseek": "deepseek-chat",
			"openai":   "gpt-4o",
		},
	}}
	auto := autoPolicy{}
	cases := []struct {
		name    string
		profile RequestProfile
		// 期望逐级降级后命中的目标
		wantProvider, wantModel string
	}{
		{"短输入→economy", RequestProfile{InputTokens: 100}, "ollama", "qwen3.5:9b"},
		{"中输入→standard", RequestProfile{InputTokens: 3000}, "deepseek", "deepseek-chat"},
		{"长输入→premium", RequestProfile{InputTokens: 9000}, "openai", "gpt-4o"},
		{"工具调用→premium", RequestProfile{InputTokens: 10, HasTools: true}, "openai", "gpt-4o"},
	}
	for _, c := range cases {
		tier := auto.evalTier(c.profile)
		i := tierIndex(tier)
		var got *RouteResult
		var ok bool
		for ; i >= 0; i-- {
			if got, ok = e.autoTargetFor(tierOrder[i], auto, nil); ok {
				break
			}
		}
		if !ok {
			t.Fatalf("%s: no target found for tier=%s", c.name, tier)
		}
		if got.Provider != c.wantProvider || got.Model != c.wantModel {
			t.Errorf("%s: got %s/%s, want %s/%s", c.name, got.Provider, got.Model, c.wantProvider, c.wantModel)
		}
	}
}

func TestAutoDowngradeWhenTierMissing(t *testing.T) {
	// 只有 economy 渠道：premium 需求也降级到 economy（可用性优先于成本匹配）
	e := &Engine{lookup: &stubLookup{
		tiers:  map[string][]string{"economy": {"ollama"}},
		models: map[string]string{"ollama": "qwen3.5:9b"},
	}}
	for tierIdx := 2; tierIdx >= 0; tierIdx-- {
		i := tierIdx
		var got *RouteResult
		var ok bool
		for ; i >= 0; i-- {
			if got, ok = e.autoTargetFor(tierOrder[i], autoPolicy{}, nil); ok {
				break
			}
		}
		if !ok || got.Provider != "ollama" {
			t.Fatalf("tier %d: must downgrade to ollama, got ok=%v res=%+v", tierIdx, ok, got)
		}
	}
}
