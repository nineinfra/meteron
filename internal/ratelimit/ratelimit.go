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

// slidingWindowSuffix 滑动窗口 v2 的 key 后缀。
// v2 使用独立 key：v1 的 ":z"/":h" 数据在窗口 TTL 后自然过期，无需手工清理。
const slidingWindowSuffix = ":sw"

// rateLimitLua 滑动窗口（单 ZSET 承载全部状态：member = "cost:唯一id"，score = 时间戳 ms）。
// KEYS[1]=zset；ARGV[1]=now_ms, ARGV[2]=window_ms, ARGV[3]=limit, ARGV[4]=cost, ARGV[5]=唯一 id。
//
// v1 为 ZSET+HASH 且逐条 HGET 求和：窗口内 N 个请求就是 N 次 redis.call，
// 高 RPM 下单次限流检查要执行数千次调用，直接阻塞 Redis 单线程（Lua 原子执行期间）。
// v2 把 cost 编码进 member，一次 ZRANGE 后在 Lua 内求和，
// Redis 调用数恒为 4（与窗口内请求数无关），且不再需要 HASH（少一个 key 与一次 TTL 维护）。
const rateLimitLua = `
local min = tonumber(ARGV[1]) - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', min)
local total = 0
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
for _, m in ipairs(members) do
    local sep = string.find(m, ':', 1, true)
    if sep then
        total = total + (tonumber(string.sub(m, 1, sep - 1)) or 0)
    end
end
if total + tonumber(ARGV[4]) > tonumber(ARGV[3]) then
    return 0
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4] .. ':' .. ARGV[5])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`

// acquireLua 并发信号量获取：超限拒绝；每次获取都刷新兜底 TTL。
// KEYS[1]=counter；ARGV[1]=limit, ARGV[2]=ttl_ms。
//
// TTL 必须在每次获取时刷新（v1 仅在首个许可 v==1 时设置）：
// 计数器长期不为零时 TTL 到期会直接删除 key，下一次获取从 0 重新开始计数，
// 而此前的许可仍被持有 —— 并发上限被静默突破。
const acquireLua = `
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
if cur + 1 > tonumber(ARGV[1]) then
    return 0
end
redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
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
	rdb           *redis.Client
	rateScript    *redis.Script
	acquireScript *redis.Script
	releaseScript *redis.Script
	// nowFunc 可注入时钟：限流窗口以客户端时间判定，测试需可控时间。
	nowFunc func() time.Time
}

func NewRedisLimiter(rdb *redis.Client) *RedisLimiter {
	return &RedisLimiter{
		rdb:           rdb,
		rateScript:    redis.NewScript(rateLimitLua),
		acquireScript: redis.NewScript(acquireLua),
		releaseScript: redis.NewScript(releaseLua),
		nowFunc:       time.Now,
	}
}

func (l *RedisLimiter) AllowRate(ctx context.Context, key string, limit int64, cost int64, window time.Duration) (bool, error) {
	if limit <= 0 {
		return true, nil // 不限流
	}
	nowMS := l.nowFunc().UnixMilli()
	res, err := l.rateScript.Run(ctx, l.rdb,
		[]string{key + slidingWindowSuffix},
		nowMS, window.Milliseconds(), limit, cost, uuid.NewString()).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// staleTTL 并发许可的兜底 TTL：
// 长流式响应（本地模型可达数分钟）期间许可必须保持持有；TTL 仅在进程崩溃
// 无法 release 时兜底回收，取 10 分钟足够覆盖极端推理时长。
// 持有期间由 keepAlive 周期续期，正常路径不会因 TTL 到期而丢失许可。
const staleTTL = 10 * time.Minute

func (l *RedisLimiter) Acquire(ctx context.Context, key string, limit int64) (release func(), ok bool, err error) {
	if limit <= 0 {
		return func() {}, true, nil // 不限流
	}
	res, err := l.acquireScript.Run(ctx, l.rdb, []string{key}, limit, staleTTL.Milliseconds()).Int64()
	if err != nil {
		return nil, false, err
	}
	if res != 1 {
		return nil, false, nil
	}

	// 持有期间持续续期：并发打满时可能长时间没有新的 Acquire（无人刷新 TTL），
	// 而长流式响应又可能超过 staleTTL —— key 过期会让计数归零、并发上限被突破。
	// 进程崩溃时心跳 goroutine 随之消失，许可仍由 staleTTL 兜底回收。
	stop := make(chan struct{})
	var once sync.Once
	go l.keepAlive(key, stop)

	// release 由 once 保护：重复调用只释放一次。
	// v1 未做幂等保护，重复 release 会多扣一次计数（把其他持有者的许可也扣掉）。
	release = func() {
		once.Do(func() {
			close(stop)
			if _, err := l.releaseScript.Run(context.Background(), l.rdb, []string{key}).Result(); err != nil {
				// 释放失败仅记录（许可靠兜底 TTL 回收），不应中断响应路径
				_ = err
			}
		})
	}
	return release, true, nil
}

// keepAlive 持有期间周期性续期许可计数 key，直到 stop 关闭。
func (l *RedisLimiter) keepAlive(key string, stop <-chan struct{}) {
	t := time.NewTicker(staleTTL / 4)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			_ = l.rdb.Expire(context.Background(), key, staleTTL).Err()
		}
	}
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
