package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// reserveLua 原子预留扣减（附录A扩展版）：
// KEYS[1]=pool key；ARGV[1]=预留金额（微单位整数）；ARGV[2]=key 不存在时的 PG 初始化值。
// 返回 {1, 剩余} 或 {0, 当前可用}。
const reserveLua = `
local key = KEYS[1]
local reserve = tonumber(ARGV[1])
local cur = redis.call('GET', key)
local avail
if cur then
    avail = tonumber(cur)
else
    avail = tonumber(ARGV[2])
    redis.call('SET', key, tostring(avail))
end
if avail < reserve then
    return {0, avail}
end
local left = avail - reserve
redis.call('SET', key, tostring(left))
return {1, left}
`

// addbackLua 归还预留额度：key 缺失返回 -1（调用方需从 PG 重新水化，PG 为可信源）。
const addbackLua = `
local key = KEYS[1]
local amt = tonumber(ARGV[1])
local cur = redis.call('GET', key)
if not cur then
    return -1
end
local v = tonumber(cur) + amt
redis.call('SET', key, tostring(v))
return v
`

// releaseLua 释放分布式锁（比较 token 后删除）。
const releaseLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`

// Redis 封装预算缓存与分布式锁。
type Redis struct {
	Client        *redis.Client
	reserveScript *redis.Script
	addbackScript *redis.Script
	releaseScript *redis.Script
}

func NewRedis(ctx context.Context, addr string) (*Redis, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Redis{
		Client:        client,
		reserveScript: redis.NewScript(reserveLua),
		addbackScript: redis.NewScript(addbackLua),
		releaseScript: redis.NewScript(releaseLua),
	}, nil
}

// Reserve 原子预留：ok=false 表示预算不足，avail 返回当前可用额度（微单位）。
func (r *Redis) Reserve(ctx context.Context, key string, amountMicro int64, initAvailMicro int64) (ok bool, avail int64, err error) {
	res, err := r.reserveScript.Run(ctx, r.Client, []string{key}, amountMicro, initAvailMicro).Result()
	if err != nil {
		return false, 0, err
	}
	arr, okArr := res.([]interface{})
	if !okArr || len(arr) != 2 {
		return false, 0, fmt.Errorf("redis reserve unexpected result: %v", res)
	}
	flag, _ := arr[0].(int64)
	avail, _ = arr[1].(int64)
	return flag == 1, avail, nil
}

// AddBack 归还额度（微单位）。key 缺失返回 hydrated=false，调用方应以 PG 可信源重新初始化。
func (r *Redis) AddBack(ctx context.Context, key string, amountMicro int64) (value int64, hydrated bool, err error) {
	v, err := r.addbackScript.Run(ctx, r.Client, []string{key}, amountMicro).Int64()
	if err != nil {
		return 0, false, err
	}
	if v == -1 {
		return 0, false, nil
	}
	return v, true, nil
}

// SetAvailIfAbsent key 缺失时用 PG 计算值初始化（水化）。
func (r *Redis) SetAvailIfAbsent(ctx context.Context, key string, availMicro int64) error {
	return r.Client.SetNX(ctx, key, availMicro, 0).Err()
}

// GetAvail 读取当前可用额度（微单位），key 不存在返回 (0,false,nil)。
func (r *Redis) GetAvail(ctx context.Context, key string) (int64, bool, error) {
	v, err := r.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var n int64
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return 0, false, fmt.Errorf("bad redis avail value %q: %w", v, err)
	}
	return n, true, nil
}

// ForceSetAvail 强制覆盖可用额度（用于每小时缓存漂移校正，PG 为准）。
func (r *Redis) ForceSetAvail(ctx context.Context, key string, availMicro int64) error {
	return r.Client.Set(ctx, key, availMicro, 0).Err()
}

// DistLock 获取 Redis 分布式锁（SET NX PX）。
func (r *Redis) DistLock(ctx context.Context, key string, ttl time.Duration) (token string, ok bool, err error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", false, err
	}
	token = hex.EncodeToString(b)
	ok, err = r.Client.SetNX(ctx, key, token, ttl).Result()
	return token, ok, err
}

// Unlock 释放分布式锁（仅持有者 token 匹配才删除）。
func (r *Redis) Unlock(ctx context.Context, key, token string) error {
	_, err := r.releaseScript.Run(ctx, r.Client, []string{key}, token).Result()
	return err
}
