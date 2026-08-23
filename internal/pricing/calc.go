package pricing

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// EventCost 单事件计价（网关 confirm 实际消耗即用此值）：
// - flat：精确
// - tiered：以首档价格近似（无法得知累计档位；出账以 BillingWorker 累计阶梯精确重算为准）
func EventCost(s *Snapshot, inputTokens, outputTokens, cachedTokens uint64) decimal.Decimal {
	if s == nil {
		return decimal.Zero
	}
	if s.PricingModel == "tiered" {
		return applyDiscount(mulTokens(s.InputUnitPrice, inputTokens).
			Add(mulTokens(s.OutputUnitPrice, outputTokens)).
			Add(mulTokens(s.CachedInputUnitPrice, cachedTokens)), s.Discount)
	}
	return applyDiscount(mulTokens(s.InputUnitPrice, inputTokens).
		Add(mulTokens(s.OutputUnitPrice, outputTokens)).
		Add(mulTokens(s.CachedInputUnitPrice, cachedTokens)), s.Discount)
}

// EventCostCumulative 阶梯累计计价（BillingWorker/深度对账使用，与出账口径一致）。
// cumBefore 为该客户该产品在本次出账周期内已累计的 token 数（input+output）；
// 事件 token 在档位边界处自动切分；返回 cost 与累计后的 token 数。
func EventCostCumulative(s *Snapshot, cumBefore, inputTokens, outputTokens, cachedTokens uint64) (decimal.Decimal, uint64) {
	if s == nil {
		return decimal.Zero, cumBefore + inputTokens + outputTokens
	}
	tokens := inputTokens + outputTokens
	var cost decimal.Decimal
	if s.PricingModel != "tiered" || len(s.Tiers) == 0 {
		cost = mulTokens(s.InputUnitPrice, inputTokens).
			Add(mulTokens(s.OutputUnitPrice, outputTokens))
	} else {
		pos := cumBefore
		remaining := tokens
		var lower uint64
		for _, tier := range s.Tiers {
			if remaining == 0 {
				break
			}
			upper := uint64(0)
			if tier.Upper != nil && *tier.Upper >= 0 {
				upper = uint64(*tier.Upper)
				if upper <= lower { // 非法配置：忽略该档
					continue
				}
			} else {
				upper = 0 // 标记无穷档
			}
			var span uint64
			if upper == 0 { // 无穷档
				span = remaining
			} else {
				if pos >= upper {
					lower = upper
					continue
				}
				span = minU64(upper-pos, remaining)
			}
			cost = cost.Add(mulTokens(tier.UnitPrice, span))
			pos += span
			remaining -= span
			if tier.Upper != nil {
				lower = uint64(*tier.Upper)
			}
		}
	}
	cost = cost.Add(mulTokens(s.CachedInputUnitPrice, cachedTokens))
	return applyDiscount(cost, s.Discount), cumBefore + tokens
}

func mulTokens(priceStr string, tokens uint64) decimal.Decimal {
	if tokens == 0 {
		return decimal.Zero
	}
	p, err := decimal.NewFromString(priceStr)
	if err != nil {
		return decimal.Zero
	}
	return p.Mul(decimal.NewFromInt(int64(tokens)))
}

func applyDiscount(d decimal.Decimal, discountStr string) decimal.Decimal {
	discount, err := decimal.NewFromString(discountStr)
	if err != nil || discount.IsZero() {
		return d
	}
	return d.Mul(decimal.NewFromInt(1).Sub(discount))
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// ValidateConfig 校验 pricing_config 合法性（管理 API 创建规则时调用）。
func ValidateConfig(ptype string, cfg *PricingConfig) error {
	switch ptype {
	case "flat":
		if cfg.UnitPrice == "" && (cfg.InputUnitPrice == "" || cfg.OutputUnitPrice == "") {
			return fmt.Errorf("flat 定价需提供 unit_price 或 input/output_unit_price")
		}
		for _, p := range []string{cfg.UnitPrice, cfg.InputUnitPrice, cfg.OutputUnitPrice, cfg.CachedInputUnitPrice} {
			if p == "" {
				continue
			}
			if _, err := decimal.NewFromString(p); err != nil {
				return fmt.Errorf("非法单价 %q: %v", p, err)
			}
		}
	case "tiered":
		if len(cfg.Tiers) == 0 {
			return fmt.Errorf("tiered 定价至少需要一档")
		}
		for i, t := range cfg.Tiers {
			if t.UnitPrice == "" {
				return fmt.Errorf("第%d档缺少 unit_price", i+1)
			}
			if _, err := decimal.NewFromString(t.UnitPrice); err != nil {
				return fmt.Errorf("第%d档非法单价 %q: %v", i+1, t.UnitPrice, err)
			}
			if t.Upper != nil {
				if i == 0 && *t.Upper <= 0 {
					return fmt.Errorf("首档 upper 必须为正数")
				}
				if i > 0 && cfg.Tiers[i-1].Upper != nil && *t.Upper <= *cfg.Tiers[i-1].Upper {
					return fmt.Errorf("第%d档 upper 必须大于上一档", i+1)
				}
			}
		}
	default:
		return fmt.Errorf("未知定价类型 %q", ptype)
	}
	return nil
}
