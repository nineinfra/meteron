package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/metrics"
	"github.com/meteron/meteron/internal/store"
)

// Reservation 预算预留。
type Reservation struct {
	ID              uuid.UUID
	TenantID        uuid.UUID
	CustomerID      uuid.UUID
	PoolID          uuid.UUID
	TransactionID   uuid.UUID
	EstimatedAmount decimal.Decimal
	ReservedAmount  decimal.Decimal
	ActualAmount    decimal.Decimal
	Status          string
	ExpiresAt       time.Time
}

// ReservationAction 状态机操作。
type ReservationAction string

const (
	ActionConfirm ReservationAction = "confirm"
	ActionRelease ReservationAction = "release"
	ActionExpire  ReservationAction = "expire"
)

// 状态常量（与 PG 枚举一致）。
const (
	StatusPending   = "pending"
	StatusConfirmed = "confirmed"
	StatusReleased  = "released"
	StatusExpired   = "expired"
)

// CanTransition 严格状态转换表（详细设计文档 §3）。
// 返回目标状态；不允许返回空串。
func CanTransition(from string, action ReservationAction) string {
	if from == StatusPending {
		switch action {
		case ActionConfirm:
			return StatusConfirmed
		case ActionRelease:
			return StatusReleased
		case ActionExpire:
			return StatusExpired
		}
	}
	return "" // 其余全部非法：confirmed/released/expired 均为终态
}

// transitionErr 终态下的业务错误：expired → 1001，其余 → 1004。
func transitionErr(current string) error {
	if current == StatusExpired {
		return common.ErrReservationExpired
	}
	return common.ErrInvalidReservationTransition.Wrap(fmt.Errorf("current status=%s", current))
}

// Service 预算预留服务：Redis Lua 原子扣减 + PG 行锁双重写入与故障补偿。
type Service struct {
	store *Store
	redis *store.Redis
	db    *pgxpool.Pool
	log   *slog.Logger
}

func NewService(st *Store, rds *store.Redis, db *pgxpool.Pool, log *slog.Logger) *Service {
	return &Service{store: st, redis: rds, db: db, log: log}
}

// Store 暴露底层 Store（后台任务复用查询）。
func (s *Service) Store() *Store { return s.store }

func availKey(poolID uuid.UUID) string {
	return fmt.Sprintf("budget:avail:%s", poolID)
}

var errInsufficientPG = errors.New("insufficient budget (pg)")
var errDuplicateTxn = errors.New("duplicate transaction")

// Reserve 预算预留（Redis 先行、PG 行锁保护、双重补偿）。
//
// 故障矩阵（详细设计文档 附录D）：
//   - Redis 成功、PG 失败：内部重试；重试耗尽自动 release Redis 预留，拒绝请求，防止预算泄露
//   - PG 成功、Redis 失败：请求拒绝（Redis 不可用时拒绝新调用，保护预算）
//   - 预算不足：返回 ErrBudgetExhausted，由调用方执行 exhaust_policy
func (s *Service) Reserve(ctx context.Context, pool *Pool, estimated decimal.Decimal, txnID uuid.UUID, ttl time.Duration) (*Reservation, error) {
	if estimated.IsNegative() {
		return nil, common.ErrBadRequest.Wrap(errors.New("negative estimated amount"))
	}
	key := availKey(pool.ID)
	estMicro := common.ToMicro(estimated)

	// 水化初始化值：PG 实时计算（不含本笔新预留）
	spent, pending, err := s.store.SpentAndPending(ctx, pool.ID)
	if err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	initAvail := common.ToMicro(pool.MaxBudget.Sub(spent).Sub(pending))

	ok, avail, err := s.redis.Reserve(ctx, key, estMicro, initAvail)
	if err != nil {
		// Redis 不可用：拒绝请求，不放行新调用（保护预算）
		return nil, common.ErrInternal.Wrap(fmt.Errorf("redis reserve: %w", err))
	}
	if !ok {
		return nil, common.ErrBudgetExhausted.Wrap(fmt.Errorf("available=%s", common.FromMicro(avail)))
	}

	// PG 写入（行锁 + 余额复核），失败内部重试
	res, err := s.insertReservation(ctx, pool, estimated, txnID, ttl)
	if err != nil {
		const retries = 2
		for i := 0; i < retries; i++ {
			res, err = s.insertReservation(ctx, pool, estimated, txnID, ttl)
			if err == nil {
				break
			}
		}
	}
	if err != nil {
		// 重试耗尽：归还 Redis 预留，防止预算泄露
		s.rollbackRedis(ctx, key, estMicro)
		if errors.Is(err, errInsufficientPG) {
			return nil, common.ErrBudgetExhausted
		}
		if errors.Is(err, errDuplicateTxn) {
			return nil, common.ErrDuplicateTransaction
		}
		return nil, common.ErrInternal.Wrap(err)
	}

	// 告警水位检查
	if pool.WarningThreshold != nil {
		remaining := common.FromMicro(avail)
		if remaining.LessThanOrEqual(*pool.WarningThreshold) {
			s.log.Warn("budget pool below warning threshold",
				"pool_id", pool.ID, "available", remaining, "warning_threshold", *pool.WarningThreshold)
		}
	}
	return res, nil
}

// insertReservation PG 事务：SELECT...FOR UPDATE 锁定 budget_pool 行（防并发超卖），
// 复核余额后插入 pending 预留。
func (s *Service) insertReservation(ctx context.Context, pool *Pool, estimated decimal.Decimal, txnID uuid.UUID, ttl time.Duration) (*Reservation, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 行锁：同一 budget_pool 的并发 reserve 串行化
	var maxStr string
	if err := tx.QueryRow(ctx,
		`SELECT max_budget::text FROM budget_pools WHERE id = $1::uuid FOR UPDATE`,
		pool.ID.String()).Scan(&maxStr); err != nil {
		return nil, err
	}
	maxBudget, err := decimal.NewFromString(maxStr)
	if err != nil {
		return nil, err
	}

	var spentStr, pendingStr *string
	if err := tx.QueryRow(ctx, `
SELECT
  (SELECT COALESCE(SUM(actual_amount),0) FROM budget_reservations WHERE pool_id=$1::uuid AND status='confirmed'),
  (SELECT COALESCE(SUM(reserved_amount),0) FROM budget_reservations WHERE pool_id=$1::uuid AND status='pending')`,
		pool.ID.String()).Scan(&spentStr, &pendingStr); err != nil {
		return nil, err
	}
	spent, pending := decimal.Zero, decimal.Zero
	if spentStr != nil {
		spent, _ = decimal.NewFromString(*spentStr)
	}
	if pendingStr != nil {
		pending, _ = decimal.NewFromString(*pendingStr)
	}
	if maxBudget.Sub(spent).Sub(pending).LessThan(estimated) {
		return nil, errInsufficientPG
	}

	var (
		idStr   string
		expires time.Time
	)
	err = tx.QueryRow(ctx, `
INSERT INTO budget_reservations
  (tenant_id, customer_id, pool_id, transaction_id, estimated_amount, reserved_amount, status, expires_at)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::numeric, $6::numeric, 'pending', NOW() + $7::interval)
RETURNING id::text, expires_at`,
		pool.TenantID.String(), pool.CustomerID.String(), pool.ID.String(),
		txnID.String(), estimated.String(), estimated.String(), fmt.Sprintf("%d seconds", int(ttl.Seconds()))).
		Scan(&idStr, &expires)
	if err != nil {
		if isDupErr(err) {
			return nil, errDuplicateTxn
		}
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, err
	}
	return &Reservation{
		ID:              id,
		TenantID:        pool.TenantID,
		CustomerID:      pool.CustomerID,
		PoolID:          pool.ID,
		TransactionID:   txnID,
		EstimatedAmount: estimated,
		ReservedAmount:  estimated,
		Status:          StatusPending,
		ExpiresAt:       expires,
	}, nil
}

// Confirm 确认消耗（幂等）：拿到真实 usage 后确认实际金额，差额自动归还（= confirm + release 差额）。
// 仅更新 Reservation；LedgerEntry 由 BillingRun 异步生成。
func (s *Service) Confirm(ctx context.Context, tenantID, reservationID uuid.UUID, actual decimal.Decimal) (*Reservation, error) {
	if actual.IsNegative() {
		return nil, common.ErrBadRequest.Wrap(errors.New("negative actual amount"))
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	res, err := s.lockReservation(ctx, tx, tenantID, reservationID)
	if err != nil {
		return nil, err
	}

	switch res.Status {
	case StatusConfirmed:
		// 幂等：重复确认同一金额不产生副作用
		if res.ActualAmount.Equal(actual) {
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return res, nil
		}
		return nil, common.ErrInvalidReservationTransition.Wrap(
			fmt.Errorf("already confirmed with amount %s", res.ActualAmount))
	case StatusPending:
		// 允许
	default:
		return nil, transitionErr(res.Status)
	}

	if _, err := tx.Exec(ctx, `
UPDATE budget_reservations
SET status='confirmed', actual_amount=$1::numeric, confirmed_at=NOW()
WHERE id=$2::uuid AND status='pending'`, actual.String(), reservationID.String()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	res.Status = StatusConfirmed
	res.ActualAmount = actual

	// Redis 归还差额（estimated - actual）
	diff := res.EstimatedAmount.Sub(actual)
	s.addBack(ctx, res.PoolID, diff)

	metrics.BudgetReservationConfirmedTotal.Inc()
	return res, nil
}

// Release 显式释放全部预留（幂等）：请求失败未产生 usage 时调用。
func (s *Service) Release(ctx context.Context, tenantID, reservationID uuid.UUID) (*Reservation, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	res, err := s.lockReservation(ctx, tx, tenantID, reservationID)
	if err != nil {
		return nil, err
	}

	switch res.Status {
	case StatusReleased:
		if err := tx.Commit(ctx); err != nil { // 幂等：已释放直接返回
			return nil, err
		}
		return res, nil
	case StatusPending:
		// 允许
	default:
		return nil, transitionErr(res.Status)
	}

	if _, err := tx.Exec(ctx, `
UPDATE budget_reservations
SET status='released', released_at=NOW()
WHERE id=$1::uuid AND status='pending'`, reservationID.String()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	res.Status = StatusReleased

	s.addBack(ctx, res.PoolID, res.ReservedAmount)
	metrics.BudgetReservationReleasedTotal.Inc()
	return res, nil
}

// ExpirePendingBatch 过期释放批量处理（ScheduledWorker 调用，须持分布式锁）。
// SELECT ... FOR UPDATE SKIP LOCKED 保证多实例安全；幂等。
func (s *Service) ExpirePendingBatch(ctx context.Context, limit int) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
SELECT id::text, pool_id::text, reserved_amount::text
FROM budget_reservations
WHERE status='pending' AND expires_at < NOW()
ORDER BY expires_at
LIMIT $1
FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id      uuid.UUID
		poolID  uuid.UUID
		reserved decimal.Decimal
	}
	var batch []expired
	for rows.Next() {
		var idStr, poolStr, amtStr string
		if err := rows.Scan(&idStr, &poolStr, &amtStr); err != nil {
			rows.Close()
			return 0, err
		}
		id, _ := uuid.Parse(idStr)
		poolID, _ := uuid.Parse(poolStr)
		amt, _ := decimal.NewFromString(amtStr)
		batch = append(batch, expired{id: id, poolID: poolID, reserved: amt})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	for _, e := range batch {
		if _, err := tx.Exec(ctx, `
UPDATE budget_reservations SET status='expired' WHERE id=$1::uuid AND status='pending'`,
			e.id.String()); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	for _, e := range batch {
		s.addBack(ctx, e.poolID, e.reserved)
	}
	metrics.BudgetReservationExpiredTotal.Add(float64(len(batch)))
	return len(batch), nil
}

// GetReservation 查询预留（管理 API）。
func (s *Service) GetReservation(ctx context.Context, tenantID, reservationID uuid.UUID) (*Reservation, error) {
	return s.queryReservation(ctx, s.db, tenantID, reservationID)
}

// ResyncPoolCache 轻量对账：以 PG 可信源校正 Redis 缓存漂移（每小时执行）。
func (s *Service) ResyncPoolCache(ctx context.Context, pool *Pool) (drift int64, err error) {
	spent, pending, err := s.store.SpentAndPending(ctx, pool.ID)
	if err != nil {
		return 0, err
	}
	pgAvail := common.ToMicro(pool.MaxBudget.Sub(spent).Sub(pending))
	key := availKey(pool.ID)
	redisAvail, exists, err := s.redis.GetAvail(ctx, key)
	if err != nil {
		return 0, err
	}
	if !exists {
		if err := s.redis.SetAvailIfAbsent(ctx, key, pgAvail); err != nil {
			return 0, err
		}
		return 0, nil
	}
	drift = redisAvail - pgAvail
	if drift != 0 {
		if err := s.redis.ForceSetAvail(ctx, key, pgAvail); err != nil {
			return drift, err
		}
		s.log.Warn("budget cache drift corrected", "pool_id", pool.ID,
			"redis", redisAvail, "pg", pgAvail, "drift_micros", drift)
		metrics.BudgetCacheDriftMicros.WithLabelValues(pool.ID.String()).Set(float64(drift))
	}
	return drift, nil
}

// CacheAvail 读取 Redis 缓存可用额度（可能缺失）。
func (s *Service) CacheAvail(ctx context.Context, poolID uuid.UUID) (int64, bool, error) {
	return s.redis.GetAvail(ctx, availKey(poolID))
}

// ---- internal ----

func (s *Service) lockReservation(ctx context.Context, tx pgx.Tx, tenantID, reservationID uuid.UUID) (*Reservation, error) {
	return s.queryReservation(ctx, tx, tenantID, reservationID, "FOR UPDATE")
}

func (s *Service) queryReservation(ctx context.Context, q queryer, tenantID, reservationID uuid.UUID, lock ...string) (*Reservation, error) {
	sql := `
SELECT id::text, tenant_id::text, customer_id::text, pool_id::text, transaction_id::text,
       estimated_amount::text, reserved_amount::text, actual_amount::text, status::text, expires_at
FROM budget_reservations
WHERE id = $1::uuid AND tenant_id = $2::uuid`
	if len(lock) > 0 {
		sql += " " + lock[0]
	}
	var (
		idStr, tidStr, cidStr, pidStr, txnStr, estStr, resStr, status string
		actualStr                                                     *string
		expiresAt                                                     time.Time
	)
	err := q.QueryRow(ctx, sql, reservationID.String(), tenantID.String()).
		Scan(&idStr, &tidStr, &cidStr, &pidStr, &txnStr, &estStr, &resStr, &actualStr, &status, &expiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, common.ErrNotFound.Wrap(fmt.Errorf("reservation %s", reservationID))
		}
		return nil, err
	}
	r := &Reservation{Status: status, ExpiresAt: expiresAt}
	if r.ID, err = uuid.Parse(idStr); err != nil {
		return nil, err
	}
	if r.TenantID, err = uuid.Parse(tidStr); err != nil {
		return nil, err
	}
	if r.CustomerID, err = uuid.Parse(cidStr); err != nil {
		return nil, err
	}
	if r.PoolID, err = uuid.Parse(pidStr); err != nil {
		return nil, err
	}
	if r.TransactionID, err = uuid.Parse(txnStr); err != nil {
		return nil, err
	}
	if r.EstimatedAmount, err = decimal.NewFromString(estStr); err != nil {
		return nil, err
	}
	if r.ReservedAmount, err = decimal.NewFromString(resStr); err != nil {
		return nil, err
	}
	if actualStr != nil {
		if r.ActualAmount, err = decimal.NewFromString(*actualStr); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// addBack 归还额度到 Redis；key 缺失时从 PG 水化（PG 为可信源）。
func (s *Service) addBack(ctx context.Context, poolID uuid.UUID, amount decimal.Decimal) {
	if amount.IsZero() {
		return
	}
	key := availKey(poolID)
	micro := common.ToMicro(amount)
	if micro == 0 {
		return
	}
	_, hydrated, err := s.redis.AddBack(ctx, key, micro)
	if err != nil {
		// Redis 故障不阻断主流程：PG 状态已提交，缓存漂移由每小时 ResyncPoolCache 校正
		s.log.Error("redis addback failed, will resync later", "pool_id", poolID, "err", err)
		return
	}
	if !hydrated {
		// key 缺失（Redis 重启/淘汰）：以 PG 可信源初始化。
		// SetNX 失败说明并发方已初始化，且其初始化必然晚于本次 PG 提交，无需补偿。
		spent, pending, err := s.store.SpentAndPending(ctx, poolID)
		if err != nil {
			s.log.Error("hydrate pool cache failed", "pool_id", poolID, "err", err)
			return
		}
		pool, err := s.store.GetPool(ctx, uuid.Nil, poolID)
		if err != nil || pool == nil {
			s.log.Error("hydrate pool cache: pool not found", "pool_id", poolID)
			return
		}
		avail := common.ToMicro(pool.MaxBudget.Sub(spent).Sub(pending))
		if err := s.redis.SetAvailIfAbsent(ctx, key, avail); err != nil {
			s.log.Error("hydrate pool cache set failed", "pool_id", poolID, "err", err)
		}
	}
}

// rollbackRedis PG 重试耗尽后归还 Redis 预留（防预算泄露）。
func (s *Service) rollbackRedis(ctx context.Context, key string, estMicro int64) {
	_, _, err := s.redis.AddBack(ctx, key, estMicro)
	if err != nil {
		s.log.Error("CRITICAL: redis rollback failed, budget may leak", "key", key, "err", err)
	}
}

// queryer 抽象 db/tx 查询。
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// isDupErr 判定唯一约束冲突（SQLSTATE 23505）。
func isDupErr(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}
