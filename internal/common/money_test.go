package common

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestMicroRoundtrip(t *testing.T) {
	for _, s := range []string{"0", "1", "0.000001", "0.123456", "123.456789", "99999999.999999"} {
		d, _ := decimal.NewFromString(s)
		back := FromMicro(ToMicro(d))
		if !back.Equal(d) {
			t.Errorf("roundtrip %s → %s failed", s, back)
		}
	}
}

func TestToMicroTruncates(t *testing.T) {
	// 超过微单位精度向下截断（不四舍五入）
	d, _ := decimal.NewFromString("0.1234567")
	if got := ToMicro(d); got != 123456 {
		t.Errorf("ToMicro(0.1234567) = %d, want 123456", got)
	}
}

func TestDec(t *testing.T) {
	if !Dec("1.5").Equal(decimal.NewFromFloat(1.5)) {
		t.Fatal("Dec parse failed")
	}
	if !Dec("garbage").IsZero() {
		t.Fatal("Dec invalid should be zero")
	}
}
