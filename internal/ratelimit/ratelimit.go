// Package ratelimit 双重限流（参考 sub2api）：
//   - 速率限制（RPM/TPM）：Redis ZSET+HASH 滑动窗口，按 cost 计量
//     （RPM cost=请求数，TPM cost=token 数）
//   - 并发限制：Redis 计数器信号量（客户维度 + 上游 Provider 账号维度），
//     带崩溃兜底 TTL，防止持许可进程崩溃导致许可永久泄漏
//
// Limiter 为接口抽象：生产 RedisLimiter，单测/降级可用 MemoryLimiter。
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Limiter 限流器接口。
type Limiter interface {
	// AllowRate 滑动窗口速率检查：window 内累计 cost 之和超过 limit 则拒绝。
	// limit<=0 表示不限流（恒放行）。
	AllowRate(ctx context.Context, key string, limit int64, cost int64, window time.Duration) (bool, error)
	// Acquire 并发信号量：获取一个许可，超限返回 ok=false。
	// 返回 release 必须调用（defer），limit<=0 表示不限流（release 为 no-op）。
	Acquire(ctx context.Context, key string, limit int64) (release func(), ok bool, err error)
}

// ---- Redis 实现 ----

// rateLimitLua 滑动窗口（ZSET 存时间戳，HASH 存每条 cost）：
// KEYS[1]=zset, KEYS[2]=hash；ARGV[1]=now_ms, ARGV[2]=window_ms,
// ARGV[3]=limit, ARGV[4]=cost, ARGV[5]=唯一 member id。
// 过期条目由 ZREMRANGEBYSCORE 清除（HASH 字段随 key TTL 整体过期，无独立泄漏）。
const rateLimitLua = `
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[1]) - tonumber(ARGV[2]))
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
local total = 0
for _, m in ipairs(members) do
    total = total + tonumber(redis.call('HGET', KEYS[2], m) or '0')
end
if total + tonumber(ARGV[4]) > tonumber(ARGV[3]) then
    return 0
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[5])
redis.call('HSET', KEYS[2], ARGV[5], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1
`

// acquireLua 并发信号量获取：超限拒绝；首个许可设置兜底 TTL。
// KEYS[1]=counter；ARGV[1]=limit, ARGV[2]=ttl_ms。
const acquireLua = `
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
if cur + 1 > tonumber(ARGV[1]) then
    return 0
end
local v = redis.call('INCR', KEYS[1])
if v == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 1
`

// releaseLua 并发信号量释放：DECR 后为负则钳回 0（防御重复 release）。
const releaseLua = `
local v = redis.call('DECR', KEYS[1])
if v < 0 then
    redis.call('SET', KEYS[1], '0')
    return 0
end
return v
`

// RedisLimiter Redis 实现的限流器。
type RedisLimiter struct {
	rdb          *redis.Client
	rateScript   *redis.Script
	acquireScript *redis.Script
	releaseScript *redis.Script
}

func NewRedisLimiter(rdb *redis.Client) *RedisLimiter {
	return &RedisLimiter{
		rdb:           rdb,
		rateScript:    redis.NewScript(rateLimitLua),
		acquireScript: redis.NewScript(acquireLua),
		releaseScript: redis.NewScript(releaseLua),
	}
}

func (l *RedisLimiter) AllowRate(ctx context.Context, key string, limit int64, cost int64, window time.Duration) (bool, error) {
	if limit <= 0 {
		return true, nil // 不限流
	}
	nowMS := time.Now().UnixMilli()
	res, err := l.rateScript.Run(ctx, l.rdb,
		[]string{key + ":z", key + ":h"},
		nowMS, window.Milliseconds(), limit, cost, uuid.NewString()).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (l *RedisLimiter) Acquire(ctx context.Context, key string, limit int64) (release func(), ok bool, err error) {
	if limit <= 0 {
		return func() {}, true, nil // 不限流
	}
	// 兜底 TTL：长流式响应（本地模型可达数分钟）期间许可必须保持持有；
	// TTL 仅在进程崩溃无法 release 时兜底回收，取 10 分钟足够覆盖极端推理时长。
	const staleTTL = 10 * time.Minute
	res, err := l.acquireScript.Run(ctx, l.rdb, []string{key}, limit, staleTTL.Milliseconds()).Int64()
	if err != nil {
		return nil, false, err
	}
	if res != 1 {
		return nil, false, nil
	}
	release = func() {
		if _, err := l.releaseScript.Run(context.Background(), l.rdb, []string{key}).Result(); err != nil {
			// 释放失败仅记录（许可靠兜底 TTL 回收），不应中断响应路径
			_ = err
		}
	}
	return release, true, nil
}

// ---- 内存实现（单测替身 / Redis 不可用时的降级选项）----

type rateEntry struct {
	at   time.Time
	cost int64
}

// MemoryLimiter 进程内限流器：语义与 RedisLimiter 一致，无持久化。
type MemoryLimiter struct {
	mu       sync.Mutex
	rate     map[string][]rateEntry
	conc     map[string]int64
	nowFunc  func() time.Time
}

func NewMemoryLimiter() *MemoryLimiter {
	return &MemoryLimiter{
		rate:    map[string][]rateEntry{},
		conc:    map[string]int64{},
		nowFunc: time.Now,
	}
}

func (l *MemoryLimiter) AllowRate(ctx context.Context, key string, limit int64, cost int64, window time.Duration) (bool, error) {
	if limit <= 0 {
		return true, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.nowFunc()
	cutoff := now.Add(-window)
	kept := l.rate[key][:0]
	var total int64
	for _, e := range l.rate[key] {
		if e.at.Before(cutoff) {
			continue
		}
		kept = append(kept, e)
		total += e.cost
	}
	l.rate[key] = kept
	if total+cost > limit {
		return false, nil
	}
	l.rate[key] = append(l.rate[key], rateEntry{at: now, cost: cost})
	return true, nil
}

func (l *MemoryLimiter) Acquire(ctx context.Context, key string, limit int64) (release func(), ok bool, err error) {
	if limit <= 0 {
		return func() {}, true, nil
	}
	l.mu.Lock()
	if l.conc[key] >= limit {
		l.mu.Unlock()
		return nil, false, nil
	}
	l.conc[key]++
	l.mu.Unlock()
	release = func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.conc[key] > 0 {
			l.conc[key]--
		}
	}
	return release, true, nil
}

// 接口契约由编译器保证
var (
	_ Limiter = (*RedisLimiter)(nil)
	_ Limiter = (*MemoryLimiter)(nil)
)
