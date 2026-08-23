package common

import (
	"math"

	"github.com/shopspring/decimal"
)

// Redis 预算缓存以“微单位”（1e-6）整数存储金额：
// 金额高精度（numeric(36,12)）在 PG 中保存；Redis 仅做快速路径扣减，
// 微单位整数在 int64/双精度安全范围内，避免 Lua tonumber 精度丢失。
const MicroScale = 6

// ToMicro 将 decimal 金额转为微单位整数（向下取整）。
func ToMicro(d decimal.Decimal) int64 {
	return d.Shift(MicroScale).IntPart()
}

// FromMicro 将微单位整数还原为 decimal 金额。
func FromMicro(v int64) decimal.Decimal {
	return decimal.New(v, -MicroScale)
}

// Dec 快捷构造。
func Dec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return d
}

// Min 取小。
func Min(a, b decimal.Decimal) decimal.Decimal {
	if a.LessThan(b) {
		return a
	}
	return b
}

// TokensToDec token 数量转 decimal。
func TokensToDec(v uint64) decimal.Decimal {
	return decimal.NewFromInt(int64(v)).Round(0)
}

// SafeUint64 防御性转换。
func SafeUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return uint64(v)
}
