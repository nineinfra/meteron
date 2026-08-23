// Package worker 后台补偿任务：过期预留释放、Redis-PG 缓存漂移校正。
//
// 多实例部署下通过 Redis 分布式锁保证同一任务同一时刻仅单实例执行
// （详细设计文档 §3 定时补偿：PG 为可信源，Redis TTL 仅兜底）。
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/store"
)

const (
	lockExpire      = "meteron:lock:scheduled:expire"
	lockResync      = "meteron:lock:scheduled:resync"
	expireBatchSize = 500
)

// ScheduledWorker 定时补偿任务集合。
type ScheduledWorker struct {
	db       *pgxpool.Pool
	redis    *store.Redis
	ledgerSt *ledger.Store
	ledgerSvc *ledger.Service
	cfg      *config.Config
	log      *slog.Logger
}

func NewScheduledWorker(db *pgxpool.Pool, rds *store.Redis, ledgerSvc *ledger.Service, cfg *config.Config, log *slog.Logger) *ScheduledWorker {
	return &ScheduledWorker{
		db: db, redis: rds,
		ledgerSt: ledgerSvc.Store(), ledgerSvc: ledgerSvc,
		cfg: cfg, log: log,
	}
}

// Run 启动两个独立循环，直至 ctx 取消。
func (w *ScheduledWorker) Run(ctx context.Context) error {
	go w.resyncLoop(ctx)
	return w.expireLoop(ctx)
}

// expireLoop 高频循环：过期 pending 预留 → expired，归还 Redis 额度。
func (w *ScheduledWorker) expireLoop(ctx context.Context) error {
	interval := w.cfg.Business.ScheduledInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// 分布式锁：多实例仅一个执行；锁 TTL 取扫描间隔2倍以上，任务本身幂等（SKIP LOCKED + 状态机）
			token, ok, err := w.redis.DistLock(ctx, lockExpire, 2*interval)
			if err != nil {
				w.log.Error("expire lock failed", "err", err)
				continue
			}
			if !ok {
				continue
			}
			total := 0
			for {
				n, err := w.ledgerSvc.ExpirePendingBatch(ctx, expireBatchSize)
				if err != nil {
					w.log.Error("expire pending batch failed", "err", err)
					break
				}
				total += n
				if n < expireBatchSize {
					break
				}
			}
			if total > 0 {
				w.log.Info("expired pending reservations", "count", total)
			}
			w.unlock(ctx, lockExpire, token)
		}
	}
}

// resyncLoop 低频循环：以 PG 可信源校正各预算池 Redis 缓存漂移。
func (w *ScheduledWorker) resyncLoop(ctx context.Context) {
	interval := w.cfg.Business.ResyncInterval
	if interval <= 0 {
		interval = time.Hour
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			token, ok, err := w.redis.DistLock(ctx, lockResync, 10*time.Minute)
			if err != nil {
				w.log.Error("resync lock failed", "err", err)
				continue
			}
			if !ok {
				continue
			}
			w.resyncAll(ctx)
			w.unlock(ctx, lockResync, token)
		}
	}
}

func (w *ScheduledWorker) resyncAll(ctx context.Context) {
	tenants, err := w.listTenants(ctx)
	if err != nil {
		w.log.Error("resync: list tenants failed", "err", err)
		return
	}
	for _, tenantID := range tenants {
		pools, err := w.ledgerSt.ListPools(ctx, tenantID)
		if err != nil {
			w.log.Error("resync: list pools failed", "tenant", tenantID, "err", err)
			continue
		}
		for _, p := range pools {
			if _, err := w.ledgerSvc.ResyncPoolCache(ctx, p); err != nil {
				w.log.Error("resync pool cache failed", "pool_id", p.ID, "err", err)
			}
		}
	}
}

func (w *ScheduledWorker) listTenants(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := w.db.Query(ctx, `SELECT id::text FROM tenants`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if id, err := uuid.Parse(s); err == nil {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

func (w *ScheduledWorker) unlock(ctx context.Context, key, token string) {
	if err := w.redis.Unlock(ctx, key, token); err != nil {
		w.log.Warn("unlock failed (will expire by TTL)", "key", key, "err", err)
	}
}
