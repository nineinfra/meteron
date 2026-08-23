package ledger

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
)

// EntryType 流水类型。
const (
	EntryUsageCharge    = "usage_charge"
	EntryCorrection     = "correction"
	EntryCommitDrawdown = "commit_drawdown"
)

// DB 最小化接口：*pgxpool.Pool 与 pgx.Tx 均满足（支持事务内写入）。
type DB interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// Entry 不可变账本流水：禁止 UPDATE，调整仅新增流水。
type Entry struct {
	TenantID      uuid.UUID
	CustomerID    uuid.UUID
	CommitmentID  *uuid.UUID
	PoolID        *uuid.UUID
	ReservationID *uuid.UUID
	Amount        decimal.Decimal
	EntryType     string
	SourceEventID *uuid.UUID
	BillID        *uuid.UUID
	Memo          string
}

// InsertEntry 新增流水（不可变约束：仅 INSERT）。
func InsertEntry(ctx context.Context, db DB, e Entry) error {
	var commitmentID, poolID, reservationID, sourceEventID, billID interface{}
	if e.CommitmentID != nil {
		commitmentID = e.CommitmentID.String()
	}
	if e.PoolID != nil {
		poolID = e.PoolID.String()
	}
	if e.ReservationID != nil {
		reservationID = e.ReservationID.String()
	}
	if e.SourceEventID != nil {
		sourceEventID = e.SourceEventID.String()
	}
	if e.BillID != nil {
		billID = e.BillID.String()
	}
	_, err := db.Exec(ctx, `
INSERT INTO ledger_entries
  (tenant_id, customer_id, commitment_id, pool_id, reservation_id,
   amount, entry_type, source_event_id, bill_id, memo)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
        $6::numeric, $7, $8::uuid, $9::uuid, $10)`,
		e.TenantID.String(), e.CustomerID.String(), commitmentID, poolID, reservationID,
		e.Amount.String(), e.EntryType, sourceEventID, billID, e.Memo)
	return err
}

// SumByType 汇总指定类型流水总额（对账用）。
func SumByType(ctx context.Context, db DB, tenantID uuid.UUID, entryType string, from, to time.Time) (decimal.Decimal, error) {
	var sumStr *string
	err := db.QueryRow(ctx, `
SELECT SUM(amount)::text FROM ledger_entries
WHERE tenant_id = $1::uuid AND entry_type = $2
  AND created_at >= $3 AND created_at < $4`,
		tenantID.String(), entryType, from, to).Scan(&sumStr)
	if err != nil {
		return decimal.Zero, err
	}
	if sumStr == nil {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(*sumStr)
}

// CountByType 统计指定类型流水条数（轻量对账用）。
func CountByType(ctx context.Context, db DB, tenantID uuid.UUID, entryType string, from, to time.Time) (int64, error) {
	var cnt int64
	err := db.QueryRow(ctx, `
SELECT COUNT(*) FROM ledger_entries
WHERE tenant_id = $1::uuid AND entry_type = $2
  AND created_at >= $3 AND created_at < $4`,
		tenantID.String(), entryType, from, to).Scan(&cnt)
	return cnt, err
}
