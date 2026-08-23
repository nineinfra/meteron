package meter

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func mkMsg(part int, off int64, hour int64) pendingMsg {
	return pendingMsg{
		msg:  kafka.Message{Topic: "meter-events", Partition: part, Offset: off},
		hour: hour,
	}
}

func offsets(msgs []kafka.Message) []int {
	out := make([]int, len(msgs))
	for i, m := range msgs {
		out[i] = int(m.Offset)
	}
	return out
}

// 乱序场景（修复的核心目标）：同分区内晚到的高小时事件先到（未落盘），
// 早小时事件后到（已落盘）。提交高 offset 会越过队首未落盘消息 → crash 后丢数据，
// 因此该分区必须整体不提交，留待高小时桶落盘后一并提交。
func TestCommittablePrefixOutOfOrder(t *testing.T) {
	flushed := map[int64]struct{}{12: {}}
	pending := []pendingMsg{
		mkMsg(0, 0, 13), // 分区0队首：hour13 未落盘 → 阻塞
		mkMsg(0, 1, 12), // hour12 已落盘，但被前驱阻塞 → 不可提交
		mkMsg(0, 2, 12),
		mkMsg(1, 5, 12), // 分区1队首已落盘 → 可提交
		mkMsg(1, 6, 12),
		mkMsg(1, 7, 13), // 分区1遇到未落盘 → 此后全部阻塞
		mkMsg(1, 8, 12),
	}
	toCommit, remaining := committablePrefix(pending, flushed)

	if got := offsets(toCommit); len(got) != 2 || got[0] != 5 || got[1] != 6 {
		t.Fatalf("toCommit offsets = %v, want [5 6]", got)
	}
	if len(remaining) != 5 {
		t.Fatalf("remaining = %d msgs, want 5", len(remaining))
	}
}

// 顺序场景：全部小时已落盘 → 全量提交。
func TestCommittablePrefixAllFlushed(t *testing.T) {
	flushed := map[int64]struct{}{12: {}, 13: {}}
	pending := []pendingMsg{
		mkMsg(0, 0, 12), mkMsg(0, 1, 13),
		mkMsg(1, 0, 12),
	}
	toCommit, remaining := committablePrefix(pending, flushed)
	if len(toCommit) != 3 {
		t.Fatalf("toCommit = %d, want 3", len(toCommit))
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining = %d, want 0", len(remaining))
	}
}

// 队首阻塞解除后（高小时桶落盘），此前被阻塞的低 offset 消息应能补提交。
func TestCommittablePrefixUnblocksAfterFlush(t *testing.T) {
	pending := []pendingMsg{
		mkMsg(0, 0, 13), mkMsg(0, 1, 12), mkMsg(0, 2, 12),
	}
	// 第一轮：仅 hour12 落盘，分区0 队首 hour13 阻塞 → 无可提交
	toCommit, remaining := committablePrefix(pending, map[int64]struct{}{12: {}})
	if len(toCommit) != 0 {
		t.Fatalf("round1 toCommit = %d, want 0", len(toCommit))
	}
	// 第二轮：hour13 也落盘 → 连续前缀全量提交
	toCommit, remaining = committablePrefix(remaining, map[int64]struct{}{12: {}, 13: {}})
	if got := offsets(toCommit); len(got) != 3 {
		t.Fatalf("round2 toCommit offsets = %v, want [0 1 2]", got)
	}
	if len(remaining) != 0 {
		t.Fatalf("round2 remaining = %d, want 0", len(remaining))
	}
}

// 空输入与空落盘集合的边界。
func TestCommittablePrefixEmpty(t *testing.T) {
	toCommit, remaining := committablePrefix(nil, map[int64]struct{}{12: {}})
	if len(toCommit) != 0 || len(remaining) != 0 {
		t.Fatal("empty pending should yield empty results")
	}
	toCommit, remaining = committablePrefix([]pendingMsg{mkMsg(0, 0, 12)}, nil)
	if len(toCommit) != 0 || len(remaining) != 1 {
		t.Fatal("nothing flushed should commit nothing")
	}
}
