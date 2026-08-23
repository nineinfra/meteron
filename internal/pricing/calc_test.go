package pricing

import (
	"testing"

	"github.com/shopspring/decimal"
)

func dec(t *testing.T, s string) decimal.Decimal {
	t.Helper()
	d, err := decimal.NewFromString(s)
	if err != nil {
		t.Fatalf("bad decimal %q: %v", s, err)
	}
	return d
}

func TestEventCostFlat(t *testing.T) {
	s := &Snapshot{
		PricingModel:         "flat",
		InputUnitPrice:       "0.000003",
		OutputUnitPrice:      "0.000006",
		CachedInputUnitPrice: "0.0000003",
	}
	// 1000 in + 500 out + 200 cached
	want := dec(t, "0.003").Add(dec(t, "0.003")).Add(dec(t, "0.00006"))
	got := EventCost(s, 1000, 500, 200)
	if !got.Equal(want) {
		t.Fatalf("flat cost = %s, want %s", got, want)
	}
	// nil 快照零价
	if !EventCost(nil, 1000, 500, 200).IsZero() {
		t.Fatal("nil snapshot should be zero cost")
	}
}

func TestEventCostDiscount(t *testing.T) {
	s := &Snapshot{
		PricingModel:    "flat",
		InputUnitPrice:  "0.000003",
		OutputUnitPrice: "0",
		Discount:        "0.2", // 8折
	}
	want := dec(t, "0.003").Mul(dec(t, "0.8"))
	if got := EventCost(s, 1000, 0, 0); !got.Equal(want) {
		t.Fatalf("discount cost = %s, want %s", got, want)
	}
}

func TestEventCostCumulativeTieredBoundaries(t *testing.T) {
	s := &Snapshot{
		PricingModel: "tiered",
		Tiers: []Tier{
			{Upper: ptr(int64(1000)), UnitPrice: "0.000003"},
			{Upper: nil, UnitPrice: "0.000001"},
		},
	}
	// 第一笔 600 tokens：全部落在首档
	cost1, cum := EventCostCumulative(s, 0, 300, 300, 0)
	if !cost1.Equal(dec(t, "0.0018")) {
		t.Fatalf("tier1 cost = %s, want 0.0018", cost1)
	}
	if cum != 600 {
		t.Fatalf("cum = %d, want 600", cum)
	}
	// 第二笔 600 tokens：400 落首档（累计到1000），200 落次档
	cost2, cum2 := EventCostCumulative(s, cum, 200, 400, 0)
	want := dec(t, "0.0012").Add(dec(t, "0.0002"))
	if !cost2.Equal(want) {
		t.Fatalf("tier2 cost = %s, want %s", cost2, want)
	}
	if cum2 != 1200 {
		t.Fatalf("cum2 = %d, want 1200", cum2)
	}
	// 第三笔：全部落无穷档
	cost3, _ := EventCostCumulative(s, cum2, 500, 500, 0)
	if !cost3.Equal(dec(t, "0.001")) {
		t.Fatalf("tier3 cost = %s, want 0.001", cost3)
	}
}

func TestEventCostCumulativeCached(t *testing.T) {
	s := &Snapshot{
		PricingModel:         "flat",
		InputUnitPrice:       "0.000003",
		OutputUnitPrice:      "0.000003",
		CachedInputUnitPrice: "0.0000003",
	}
	// cached tokens 单独计价，不计入累计档位
	cost, cum := EventCostCumulative(s, 0, 0, 0, 1000)
	if !cost.Equal(dec(t, "0.0003")) {
		t.Fatalf("cached cost = %s, want 0.0003", cost)
	}
	if cum != 0 {
		t.Fatalf("cached should not advance cumulative tokens, got %d", cum)
	}
}

func TestValidateConfig(t *testing.T) {
	if err := ValidateConfig("flat", &PricingConfig{InputUnitPrice: "0.000003", OutputUnitPrice: "0.000006"}); err != nil {
		t.Fatalf("valid flat rejected: %v", err)
	}
	if err := ValidateConfig("flat", &PricingConfig{}); err == nil {
		t.Fatal("flat without prices should fail")
	}
	if err := ValidateConfig("tiered", &PricingConfig{Tiers: []Tier{
		{Upper: ptr(int64(1000)), UnitPrice: "0.000003"},
		{Upper: nil, UnitPrice: "0.000001"},
	}}); err != nil {
		t.Fatalf("valid tiered rejected: %v", err)
	}
	if err := ValidateConfig("tiered", &PricingConfig{}); err == nil {
		t.Fatal("tiered without tiers should fail")
	}
	if err := ValidateConfig("tiered", &PricingConfig{Tiers: []Tier{
		{Upper: ptr(int64(2000)), UnitPrice: "0.000003"},
		{Upper: ptr(int64(1000)), UnitPrice: "0.000001"}, // upper 递减非法
	}}); err == nil {
		t.Fatal("non-increasing tier upper should fail")
	}
	if err := ValidateConfig("bogus", &PricingConfig{}); err == nil {
		t.Fatal("unknown pricing type should fail")
	}
}

// 单笔事件跨三档（0-1000 / 1000-5000 / 5000+），验证档位切分逐段精确。
func TestEventCostCumulativeCrossThreeTiers(t *testing.T) {
	s := &Snapshot{
		PricingModel: "tiered",
		Tiers: []Tier{
			{Upper: ptr(int64(1000)), UnitPrice: "0.000003"},
			{Upper: ptr(int64(5000)), UnitPrice: "0.000002"},
			{Upper: nil, UnitPrice: "0.000001"},
		},
	}
	// 单笔 6000 tokens：1000×3µ + 4000×2µ + 1000×1µ
	cost, cum := EventCostCumulative(s, 0, 6000, 0, 0)
	want := dec(t, "0.003").Add(dec(t, "0.008")).Add(dec(t, "0.001"))
	if !cost.Equal(want) {
		t.Fatalf("cross-3-tier cost = %s, want %s", cost, want)
	}
	if cum != 6000 {
		t.Fatalf("cum = %d, want 6000", cum)
	}
	// 累计已超全部有限档：整笔落无穷档
	cost2, _ := EventCostCumulative(s, 5000, 100, 100, 0)
	if !cost2.Equal(dec(t, "0.0002")) {
		t.Fatalf("infinite-tier cost = %s, want 0.0002", cost2)
	}
}

// tiered 下 cached tokens 独立计价（不占档位、不推进累计）。
func TestEventCostCumulativeTieredCachedSeparate(t *testing.T) {
	s := &Snapshot{
		PricingModel:         "tiered",
		InputUnitPrice:       "0.000003",
		OutputUnitPrice:      "0.000003",
		CachedInputUnitPrice: "0.0000003",
		Tiers: []Tier{
			{Upper: ptr(int64(1000)), UnitPrice: "0.000003"},
			{Upper: nil, UnitPrice: "0.000001"},
		},
	}
	cost, cum := EventCostCumulative(s, 0, 1000, 0, 500)
	want := dec(t, "0.003").Add(dec(t, "0.00015")) // 首档 1000 + cached 500
	if !cost.Equal(want) {
		t.Fatalf("tiered+cached cost = %s, want %s", cost, want)
	}
	if cum != 1000 {
		t.Fatalf("cum = %d, want 1000 (cached 不推进累计)", cum)
	}
}

// nil 快照：零成本且累计照常推进（防止空指针 panic）。
func TestEventCostCumulativeNilSnapshot(t *testing.T) {
	cost, cum := EventCostCumulative(nil, 100, 30, 20, 0)
	if !cost.IsZero() {
		t.Fatalf("nil snapshot cost = %s, want 0", cost)
	}
	if cum != 150 {
		t.Fatalf("cum = %d, want 150", cum)
	}
}

// 非法阶梯配置：首档 upper 非正、档间 upper 非递增、缺单价、非法 decimal。
func TestValidateConfigInvalid(t *testing.T) {
	bad := []struct {
		name  string
		ptype string
		cfg   *PricingConfig
	}{
		{"flat 非法单价", "flat", &PricingConfig{InputUnitPrice: "abc", OutputUnitPrice: "0.1"}},
		{"tiered 首档 upper 非正", "tiered", &PricingConfig{Tiers: []Tier{{Upper: ptr(int64(0)), UnitPrice: "0.1"}}}},
		{"tiered 缺单价", "tiered", &PricingConfig{Tiers: []Tier{{Upper: ptr(int64(100))}}}},
		{"tiered 非法单价", "tiered", &PricingConfig{Tiers: []Tier{{Upper: ptr(int64(100)), UnitPrice: "x"}}}},
		{"tiered upper 相等", "tiered", &PricingConfig{Tiers: []Tier{
			{Upper: ptr(int64(1000)), UnitPrice: "0.1"},
			{Upper: ptr(int64(1000)), UnitPrice: "0.05"},
		}}},
	}
	for _, c := range bad {
		if err := ValidateConfig(c.ptype, c.cfg); err == nil {
			t.Errorf("%s: should fail, got nil", c.name)
		}
	}
}

func ptr[T any](v T) *T { return &v }
