package ledger

import (
	"testing"

	"github.com/meteron/meteron/internal/common"
)

// 严格状态转换表（详细设计文档 §3）：
// pending → confirmed/released/expired；其余转换一律非法（终态不可逆）。
func TestCanTransition(t *testing.T) {
	cases := []struct {
		from   string
		action ReservationAction
		want   string
	}{
		{StatusPending, ActionConfirm, StatusConfirmed},
		{StatusPending, ActionRelease, StatusReleased},
		{StatusPending, ActionExpire, StatusExpired},
		// 终态 → 任何操作均非法
		{StatusConfirmed, ActionConfirm, ""},
		{StatusConfirmed, ActionRelease, ""},
		{StatusConfirmed, ActionExpire, ""},
		{StatusReleased, ActionConfirm, ""},
		{StatusReleased, ActionRelease, ""},
		{StatusReleased, ActionExpire, ""},
		{StatusExpired, ActionConfirm, ""},
		{StatusExpired, ActionRelease, ""},
		{StatusExpired, ActionExpire, ""},
		// 未知状态
		{"bogus", ActionConfirm, ""},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.action); got != c.want {
			t.Errorf("CanTransition(%s, %s) = %q, want %q", c.from, c.action, got, c.want)
		}
	}
}

// 非法转换错误码映射（详细设计文档 §6 错误码字典）：
// expired → 1001 RESERVATION_EXPIRED；其余终态 → 1004 INVALID_RESERVATION_STATE_TRANSITION。
func TestTransitionErrCodes(t *testing.T) {
	if err := transitionErr(StatusExpired); common.FromErr(err).Code != common.CodeReservationExpired {
		t.Fatalf("expired transition err code = %d, want %d",
			common.FromErr(err).Code, common.CodeReservationExpired)
	}
	for _, from := range []string{StatusConfirmed, StatusReleased} {
		err := transitionErr(from)
		if got := common.FromErr(err).Code; got != common.CodeInvalidReservationTransition {
			t.Fatalf("transitionErr(%s) code = %d, want %d", from, got, common.CodeInvalidReservationTransition)
		}
	}
}
