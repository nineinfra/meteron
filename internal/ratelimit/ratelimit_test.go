package ratelimit

import (
	"context"
	"testing"
	"time"
)

// newMemLimiter 注入固定时钟。
func newMemLimiter() (*MemoryLimiter, *time.Time) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	l := NewMemoryLimiter()
	l.nowFunc = func() time.Time { return now }
	return l, &now
}

func TestAllowRateUnlimitedWhenLimitZero(t *testing.T) {
	l, _ := newMemLimiter()
	for i := 0; i < 1000; i++ {
		ok, err := l.AllowRate(context.Background(), "k", 0, 1, time.Minute)
		if err != nil || !ok {
			t.Fatalf("limit=0 must always allow (i=%d): ok=%v err=%v", i, ok, err)
		}
	}
}

func TestAllowRateRequestSemantics(t *testing.T) {
	l, _ := newMemLimiter()
	ctx := context.Background()
	// RPM=3：前 3 次放行（cost=1），第 4 次拒绝
	for i := 0; i < 3; i++ {
		ok, _ := l.AllowRate(ctx, "rpm", 3, 1, time.Minute)
		if !ok {
			t.Fatalf("request %d should pass", i+1)
		}
	}
	if ok, _ := l.AllowRate(ctx, "rpm", 3, 1, time.Minute); ok {
		t.Fatal("4th request within window must be rejected")
	}
}

func TestAllowRateTokenSemantics(t *testing.T) {
	l, _ := newMemLimiter()
	ctx := context.Background()
	// TPM=1000：两个请求 600+500 tokens，第二个被拒（累计 1100 > 1000）
	if ok, _ := l.AllowRate(ctx, "tpm", 1000, 600, time.Minute); !ok {
		t.Fatal("600 tokens should pass")
	}
	if ok, _ := l.AllowRate(ctx, "tpm", 1000, 500, time.Minute); ok {
		t.Fatal("600+500 tokens must exceed TPM=1000")
	}
	// 小请求仍可放行（600+300 < 1000）
	if ok, _ := l.AllowRate(ctx, "tpm", 1000, 300, time.Minute); !ok {
		t.Fatal("600+300 tokens should pass")
	}
}

func TestAllowRateWindowSlides(t *testing.T) {
	l, now := newMemLimiter()
	ctx := context.Background()
	// t0 打满额度（3 个样本同刻 t0）
	for i := 0; i < 3; i++ {
		if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); !ok {
			t.Fatal("precondition failed")
		}
	}
	if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); ok {
		t.Fatal("should be limited inside window")
	}
	// 窗口滑过（61s）：t0 样本全部过期（cutoff=t0+1s），额度重置
	*now = now.Add(61 * time.Second)
	for i := 0; i < 3; i++ {
		if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); !ok {
			t.Fatalf("slid window should admit request %d", i+1)
		}
	}
	if ok, _ := l.AllowRate(ctx, "k", 3, 1, time.Minute); ok {
		t.Fatal("window full again must reject")
	}
}

func TestAllowRateKeysIsolated(t *testing.T) {
	l, _ := newMemLimiter()
	ctx := context.Background()
	if ok, _ := l.AllowRate(ctx, "a", 1, 1, time.Minute); !ok {
		t.Fatal("key a first request should pass")
	}
	if ok, _ := l.AllowRate(ctx, "b", 1, 1, time.Minute); !ok {
		t.Fatal("key b must be isolated from key a")
	}
}

func TestAcquireConcurrencyLimit(t *testing.T) {
	l := NewMemoryLimiter()
	ctx := context.Background()
	// 并发上限 2：第 3 个获取失败
	r1, ok1, _ := l.Acquire(ctx, "c", 2)
	r2, ok2, _ := l.Acquire(ctx, "c", 2)
	if !ok1 || !ok2 {
		t.Fatal("first two acquires should succeed")
	}
	if _, ok3, _ := l.Acquire(ctx, "c", 2); ok3 {
		t.Fatal("third acquire must fail at limit=2")
	}
	// release 一个后可再获取
	r1()
	if _, ok, _ := l.Acquire(ctx, "c", 2); !ok {
		t.Fatal("acquire after release should succeed")
	}
	r2()
}

func TestAcquireReleaseIdempotentSafe(t *testing.T) {
	l := NewMemoryLimiter()
	ctx := context.Background()
	r, ok, _ := l.Acquire(ctx, "c", 1)
	if !ok {
		t.Fatal("acquire should succeed")
	}
	r()
	r() // 重复 release 不得把计数器打成负数
	r()
	// 计数归零后仍可正常获取
	if _, ok, _ := l.Acquire(ctx, "c", 1); !ok {
		t.Fatal("counter should be 0 after release")
	}
}

func TestAcquireZeroLimitNoop(t *testing.T) {
	l := NewMemoryLimiter()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r, ok, err := l.Acquire(ctx, "c", 0)
		if err != nil || !ok {
			t.Fatalf("limit=0 must always allow: ok=%v err=%v", ok, err)
		}
		r()
	}
}

func TestAcquireKeysIsolated(t *testing.T) {
	l := NewMemoryLimiter()
	ctx := context.Background()
	if _, ok, _ := l.Acquire(ctx, "prov:ollama", 1); !ok {
		t.Fatal("provider ollama should acquire")
	}
	if _, ok, _ := l.Acquire(ctx, "prov:openai", 1); !ok {
		t.Fatal("provider openai must be independent of ollama")
	}
}

// RedisLimiter 无 Redis 环境下验证 limit<=0 短路路径不触碰网络。
func TestRedisLimiterZeroLimitShortCircuit(t *testing.T) {
	l := NewRedisLimiter(nil) // rdb=nil：limit>0 的路径会 panic/报错，limit<=0 必须短路
	ctx := context.Background()
	if ok, err := l.AllowRate(ctx, "k", 0, 1, time.Minute); err != nil || !ok {
		t.Fatalf("AllowRate limit=0 short-circuit failed: ok=%v err=%v", ok, err)
	}
	r, ok, err := l.Acquire(ctx, "k", 0)
	if err != nil || !ok || r == nil {
		t.Fatalf("Acquire limit=0 short-circuit failed: ok=%v err=%v release=%v", ok, err, r != nil)
	}
	r() // no-op release 不应触碰 nil client
}

// 并发安全：多 goroutine 同时 AllowRate/Acquire 不竞态（-race 下验证）。
func TestMemoryLimiterConcurrentSafety(t *testing.T) {
	l := NewMemoryLimiter()
	ctx := context.Background()
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 50; i++ {
				_, _ = l.AllowRate(ctx, "rate", 1000, 1, time.Minute)
				if r, ok, _ := l.Acquire(ctx, "conc", 4); ok {
					r()
				}
			}
		}()
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	// 全部释放后并发计数必须归零（release 均被调用）
	l.mu.Lock()
	got := l.conc["conc"]
	l.mu.Unlock()
	if got != 0 {
		t.Fatalf("concurrency counter after all releases = %d, want 0", got)
	}
}
