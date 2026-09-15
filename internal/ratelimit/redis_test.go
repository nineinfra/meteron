package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newRedisLimiter miniredis 上的 RedisLimiter（可注入固定时钟）。
// 覆盖 Memory 实现测不到的部分：Lua 脚本语义。
func newRedisLimiter(t *testing.T) (*RedisLimiter, *time.Time) {
	t.Helper()
	m := miniredis.RunT(t)
	t.Cleanup(m.Close)
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	l := NewRedisLimiter(redis.NewClient(&redis.Options{Addr: m.Addr()}))
	t.Cleanup(func() { _ = l.rdb.Close() })
	l.nowFunc = func() time.Time { return now }
	return l, &now
}

// cost 编码进 member（v2 单 ZSET 方案）：验证 Lua 能正确解析并累加 token 数。
func TestRedisAllowRateTokenSemantics(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()

	if ok, err := l.AllowRate(ctx, "tpm", 1000, 600, time.Minute); err != nil || !ok {
		t.Fatalf("600 tokens should pass: ok=%v err=%v", ok, err)
	}
	if ok, err := l.AllowRate(ctx, "tpm", 1000, 500, time.Minute); err != nil || ok {
		t.Fatalf("600+500 must exceed TPM=1000: ok=%v err=%v", ok, err)
	}
	// 被拒绝的请求不应计入窗口
	if ok, err := l.AllowRate(ctx, "tpm", 1000, 300, time.Minute); err != nil || !ok {
		t.Fatalf("600+300 should pass (rejected request not counted): ok=%v err=%v", ok, err)
	}
	if ok, err := l.AllowRate(ctx, "tpm", 1000, 200, time.Minute); err != nil || ok {
		t.Fatalf("600+300+200 must exceed TPM=1000: ok=%v err=%v", ok, err)
	}
}

func TestRedisAllowRateRequestSemantics(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if ok, err := l.AllowRate(ctx, "rpm", 3, 1, time.Minute); err != nil || !ok {
			t.Fatalf("request %d should pass: ok=%v err=%v", i+1, ok, err)
		}
	}
	if ok, err := l.AllowRate(ctx, "rpm", 3, 1, time.Minute); err != nil || ok {
		t.Fatalf("4th request must be rejected: ok=%v err=%v", ok, err)
	}
}

// 窗口滑动：推进客户端时钟后额度回收（过期条目被 ZREMRANGEBYSCORE 清除）。
func TestRedisAllowRateWindowSlides(t *testing.T) {
	l, now := newRedisLimiter(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); !ok {
			t.Fatal("precondition failed")
		}
	}
	if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); ok {
		t.Fatal("should be limited inside window")
	}
	*now = now.Add(61 * time.Second)
	for i := 0; i < 3; i++ {
		if ok, err := l.AllowRate(ctx, "k", 3, 1, time.Minute); err != nil || !ok {
			t.Fatalf("slid window should admit request %d: ok=%v err=%v", i+1, ok, err)
		}
	}
	if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); ok {
		t.Fatal("window full again must reject")
	}
}

func TestRedisAllowRateKeysIsolated(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()
	if ok, _ := l.AllowRate(ctx, "a", 1, 1, time.Minute); !ok {
		t.Fatal("key a first request should pass")
	}
	if ok, _ := l.AllowRate(ctx, "b", 1, 1, time.Minute); !ok {
		t.Fatal("key b must be isolated from key a")
	}
}

func TestRedisAcquireConcurrency(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()
	r1, ok1, err := l.Acquire(ctx, "c", 2)
	if err != nil || !ok1 {
		t.Fatalf("first acquire failed: ok=%v err=%v", ok1, err)
	}
	r2, ok2, err := l.Acquire(ctx, "c", 2)
	if err != nil || !ok2 {
		t.Fatalf("second acquire failed: ok=%v err=%v", ok2, err)
	}
	if _, ok3, _ := l.Acquire(ctx, "c", 2); ok3 {
		t.Fatal("third acquire must be rejected (limit=2)")
	}
	r1()
	if _, ok, _ := l.Acquire(ctx, "c", 2); !ok {
		t.Fatal("acquire after release should succeed")
	}
	r2()
}

// release 幂等：重复释放不得扣掉其他持有者的许可，否则并发上限被突破
// （limit=2 时会同时出现 3 个持有者）。
func TestRedisReleaseIdempotent(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()

	ra, ok, err := l.Acquire(ctx, "c", 2)
	if err != nil || !ok {
		t.Fatalf("acquire a failed: ok=%v err=%v", ok, err)
	}
	rb, ok, err := l.Acquire(ctx, "c", 2)
	if err != nil || !ok {
		t.Fatalf("acquire b failed: ok=%v err=%v", ok, err)
	}
	// a 释放两次（模拟 defer + 显式调用）
	ra()
	ra()

	// 此时仅 b 持有：还能再获取 1 个（c），但不能再获取第 2 个（d）
	rc, ok, err := l.Acquire(ctx, "c", 2)
	if err != nil || !ok {
		t.Fatalf("acquire c should succeed (a released): ok=%v err=%v", ok, err)
	}
	if _, ok, _ := l.Acquire(ctx, "c", 2); ok {
		t.Fatal("4th holder must be rejected: duplicate release leaked a permit")
	}
	rb()
	rc()
}

// 许可计数 key 必须带 TTL（崩溃兜底回收）。
func TestRedisAcquireSetsTTL(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()
	release, ok, err := l.Acquire(ctx, "ttl", 1)
	if err != nil || !ok {
		t.Fatalf("acquire failed: ok=%v err=%v", ok, err)
	}
	defer release()
	ttl, err := l.rdb.PTTL(ctx, "ttl").Result()
	if err != nil {
		t.Fatalf("pttl: %v", err)
	}
	if ttl <= 0 || ttl > staleTTL {
		t.Fatalf("ttl = %v, want (0, %v]", ttl, staleTTL)
	}
}

// 滑动窗口 key 同样必须带 TTL，否则低流量 key 永久驻留。
func TestRedisAllowRateSetsTTL(t *testing.T) {
	l, _ := newRedisLimiter(t)
	ctx := context.Background()
	if ok, err := l.AllowRate(ctx, "k", 10, 1, time.Minute); err != nil || !ok {
		t.Fatalf("allow rate failed: ok=%v err=%v", ok, err)
	}
	ttl, err := l.rdb.PTTL(ctx, "k"+slidingWindowSuffix).Result()
	if err != nil {
		t.Fatalf("pttl: %v", err)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ttl = %v, want (0, 1m]", ttl)
	}
}
