// Package ledger 资金账本：预算池、BudgetReservation 预留状态机、不可变流水。
//
// 重要语义区分（详细设计文档 §3）：
// - BudgetReservation.confirmed：网关侧已拿到真实 usage 并确认，≠ LedgerEntry 已入账
// - LedgerEntry.posted：异步 Worker（BillingRun）完成财务记账
package ledger

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/common"
)

// Pool 预算池。
type Pool struct {
	ID               uuid.UUID
	TenantID         uuid.UUID
	CustomerID       uuid.UUID
	TaskType         *string
	AgentID          *string
	MaxBudget        decimal.Decimal
	WarningThreshold *decimal.Decimal
	ExhaustPolicy    string // warn | block | fallback
}

// PoolBalance 预算池余额（PG 为可信源实时计算）。
type PoolBalance struct {
	PoolID           uuid.UUID       `json:"pool_id"`
	MaxBudget        decimal.Decimal `json:"max_budget"`
	SpentAmount      decimal.Decimal `json:"spent_amount"`      // Σ confirmed.actual_amount
	PendingReserved  decimal.Decimal `json:"pending_reserved"`  // Σ pending.reserved_amount
	AvailableAmount  decimal.Decimal `json:"available_amount"`  // max - spent - pending
	CacheAvailMicro  *int64          `json:"cache_avail_micro"` // Redis 缓存值（可能缺失）
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// FindPool 按（customer, task_type, agent_id）特异性匹配预算池。
func (s *Store) FindPool(ctx context.Context, tenantID, customerID uuid.UUID, taskType, agentID string) (*Pool, error) {
	var (
		idStr, cidStr, tidStr string
		maxStr                string
		warnStr               *string
		policy                string
		taskCol               *string
		agentCol              *string
	)
	err := s.db.QueryRow(ctx, `
SELECT id::text, tenant_id::text, customer_id::text, max_budget::text,
       warning_threshold::text, exhaust_policy::text, task_type, agent_id
FROM budget_pools
WHERE tenant_id = $1::uuid AND customer_id = $2::uuid AND deleted_at IS NULL
  AND (task_type = $3 OR task_type IS NULL)
  AND (agent_id = $4 OR agent_id IS NULL)
ORDER BY (task_type IS NOT NULL) DESC, (agent_id IS NOT NULL) DESC
LIMIT 1`, tenantID.String(), customerID.String(), nullStr(taskType), nullStr(agentID)).
		Scan(&idStr, &tidStr, &cidStr, &maxStr, &warnStr, &policy, &taskCol, &agentCol)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	p := &Pool{ExhaustPolicy: policy, TaskType: taskCol, AgentID: agentCol}
	if p.ID, err = uuid.Parse(idStr); err != nil {
		return nil, err
	}
	if p.TenantID, err = uuid.Parse(tidStr); err != nil {
		return nil, err
	}
	if p.CustomerID, err = uuid.Parse(cidStr); err != nil {
		return nil, err
	}
	if p.MaxBudget, err = decimal.NewFromString(maxStr); err != nil {
		return nil, err
	}
	if warnStr != nil {
		w, err := decimal.NewFromString(*warnStr)
		if err != nil {
			return nil, err
		}
		p.WarningThreshold = &w
	}
	return p, nil
}

// GetPool 按 ID 查询。
func (s *Store) GetPool(ctx context.Context, tenantID, poolID uuid.UUID) (*Pool, error) {
	var (
		idStr, cidStr, tidStr string
		maxStr                string
		warnStr               *string
		policy                string
		taskCol, agentCol     *string
	)
	err := s.db.QueryRow(ctx, `
SELECT id::text, tenant_id::text, customer_id::text, max_budget::text,
       warning_threshold::text, exhaust_policy::text, task_type, agent_id
FROM budget_pools
WHERE id = $1::uuid AND tenant_id = $2::uuid AND deleted_at IS NULL`,
		poolID.String(), tenantID.String()).
		Scan(&idStr, &tidStr, &cidStr, &maxStr, &warnStr, &policy, &taskCol, &agentCol)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, common.ErrNotFound.Wrap(fmt.Errorf("budget pool %s", poolID))
		}
		return nil, err
	}
	p := &Pool{ExhaustPolicy: policy, TaskType: taskCol, AgentID: agentCol}
	if p.ID, err = uuid.Parse(idStr); err != nil {
		return nil, err
	}
	if p.TenantID, err = uuid.Parse(tidStr); err != nil {
		return nil, err
	}
	if p.CustomerID, err = uuid.Parse(cidStr); err != nil {
		return nil, err
	}
	if p.MaxBudget, err = decimal.NewFromString(maxStr); err != nil {
		return nil, err
	}
	if warnStr != nil {
		w, err := decimal.NewFromString(*warnStr)
		if err != nil {
			return nil, err
		}
		p.WarningThreshold = &w
	}
	return p, nil
}

// ListPools 列出租户全部有效预算池（轻量对账遍历用）。
func (s *Store) ListPools(ctx context.Context, tenantID uuid.UUID) ([]*Pool, error) {
	rows, err := s.db.Query(ctx, `
SELECT id::text, tenant_id::text, customer_id::text, max_budget::text,
       warning_threshold::text, exhaust_policy::text, task_type, agent_id
FROM budget_pools
WHERE tenant_id = $1::uuid AND deleted_at IS NULL`, tenantID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Pool
	for rows.Next() {
		var (
			idStr, tidStr, cidStr, maxStr string
			warnStr                       *string
			policy                        string
			taskCol, agentCol             *string
		)
		if err := rows.Scan(&idStr, &tidStr, &cidStr, &maxStr, &warnStr, &policy, &taskCol, &agentCol); err != nil {
			return nil, err
		}
		p := &Pool{ExhaustPolicy: policy, TaskType: taskCol, AgentID: agentCol}
		if p.ID, err = uuid.Parse(idStr); err != nil {
			return nil, err
		}
		if p.TenantID, err = uuid.Parse(tidStr); err != nil {
			return nil, err
		}
		if p.CustomerID, err = uuid.Parse(cidStr); err != nil {
			return nil, err
		}
		if p.MaxBudget, err = decimal.NewFromString(maxStr); err != nil {
			return nil, err
		}
		if warnStr != nil {
			w, err := decimal.NewFromString(*warnStr)
			if err != nil {
				return nil, err
			}
			p.WarningThreshold = &w
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SpentAndPending 汇总池的已确认消耗与在途预留。
func (s *Store) SpentAndPending(ctx context.Context, poolID uuid.UUID) (spent, pending decimal.Decimal, err error) {
	var spentStr, pendingStr *string
	err = s.db.QueryRow(ctx, `
SELECT
  (SELECT COALESCE(SUM(actual_amount),0) FROM budget_reservations WHERE pool_id=$1::uuid AND status='confirmed'),
  (SELECT COALESCE(SUM(reserved_amount),0) FROM budget_reservations WHERE pool_id=$1::uuid AND status='pending')`,
		poolID.String()).Scan(&spentStr, &pendingStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	if spentStr != nil {
		spent, _ = decimal.NewFromString(*spentStr)
	}
	if pendingStr != nil {
		pending, _ = decimal.NewFromString(*pendingStr)
	}
	return spent, pending, nil
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
