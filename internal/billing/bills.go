package billing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/common"
)

// Bill 账单视图。
type Bill struct {
	ID               uuid.UUID       `json:"id"`
	TenantID         uuid.UUID       `json:"-"`
	CustomerID       uuid.UUID       `json:"customer_id"`
	ContractID       uuid.UUID       `json:"contract_id"`
	Status           string          `json:"status"`
	PeriodStart      time.Time       `json:"period_start"`
	PeriodEnd        time.Time       `json:"period_end"`
	TotalAmount      decimal.Decimal `json:"total_amount"`
	Currency         string          `json:"currency"`
	AdjustmentBillID *uuid.UUID      `json:"adjustment_bill_id,omitempty"`
	AdjustmentReason *string         `json:"adjustment_reason,omitempty"`
	FinalizedAt      *time.Time      `json:"finalized_at,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	LineItems        []LineItem      `json:"line_items,omitempty"`
}

// LineItem 账单行项目。
type LineItem struct {
	ID            uuid.UUID       `json:"id"`
	ProductID     uuid.UUID       `json:"product_id"`
	Amount        decimal.Decimal `json:"amount"`
	Quantity      decimal.Decimal `json:"quantity"`
	SnapshotRaw   string          `json:"pricing_context_snapshot"`
	MetadataRaw   string          `json:"metadata"`
}

// BillingRun 跑批任务视图。
type BillingRun struct {
	ID            uuid.UUID       `json:"id"`
	Status        string          `json:"status"`
	PeriodStart   time.Time       `json:"period_start"`
	PeriodEnd     time.Time       `json:"period_end"`
	TotalCustomers int            `json:"total_customers"`
	TotalBills    int             `json:"total_bills"`
	ErrorCount    int             `json:"error_count"`
	ErrorDetail   string          `json:"error_detail,omitempty"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
}

// FinalizeBill 账单定稿：draft → finalized（幂等：已 finalized 直接成功）。
// 定稿后禁止任何修改；调整走 amending/红冲（V1）。
func FinalizeBill(ctx context.Context, db *pgxpool.Pool, tenantID, billID uuid.UUID) (*Bill, error) {
	b, err := GetBill(ctx, db, tenantID, billID)
	if err != nil {
		return nil, err
	}
	switch b.Status {
	case "finalized":
		return b, nil // 幂等
	case "draft", "amending":
		if _, err := db.Exec(ctx, `
UPDATE bills SET status='finalized', finalized_at=NOW() WHERE id=$1::uuid AND tenant_id=$2::uuid`,
			billID.String(), tenantID.String()); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		return GetBill(ctx, db, tenantID, billID)
	default:
		return nil, common.ErrBadRequest.Wrap(fmt.Errorf("cannot finalize bill in status %s", b.Status))
	}
}

// GetBill 查询账单（含行项目）。
func GetBill(ctx context.Context, db *pgxpool.Pool, tenantID, billID uuid.UUID) (*Bill, error) {
	b := &Bill{}
	var adjID, adjReason *string
	var (
		idStr, cidStr, ctidStr string
		totalStr               string
	)
	err := db.QueryRow(ctx, `
SELECT id::text, customer_id::text, contract_id::text, status::text,
       period_start, period_end, total_amount::text, currency,
       adjustment_bill_id::text, adjustment_reason, finalized_at, created_at
FROM bills WHERE id=$1::uuid AND tenant_id=$2::uuid AND deleted_at IS NULL`,
		billID.String(), tenantID.String()).
		Scan(&idStr, &cidStr, &ctidStr, &b.Status, &b.PeriodStart, &b.PeriodEnd,
			&totalStr, &b.Currency, &adjID, &adjReason, &b.FinalizedAt, &b.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, common.ErrNotFound.Wrap(fmt.Errorf("bill %s", billID))
		}
		return nil, common.ErrInternal.Wrap(err)
	}
	if b.ID, err = uuid.Parse(idStr); err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	if b.CustomerID, err = uuid.Parse(cidStr); err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	if b.ContractID, err = uuid.Parse(ctidStr); err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	if b.TotalAmount, err = decimal.NewFromString(totalStr); err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	if adjID != nil {
		if id, err := uuid.Parse(*adjID); err == nil {
			b.AdjustmentBillID = &id
		}
	}
	b.AdjustmentReason = adjReason

	items, err := queryLineItems(ctx, db, billID)
	if err != nil {
		return nil, err
	}
	b.LineItems = items
	return b, nil
}

func queryLineItems(ctx context.Context, db *pgxpool.Pool, billID uuid.UUID) ([]LineItem, error) {
	rows, err := db.Query(ctx, `
SELECT id::text, product_id::text, amount::text, quantity::text,
       pricing_context_snapshot::text, metadata::text
FROM bill_line_items WHERE bill_id=$1::uuid ORDER BY created_at, id`,
		billID.String())
	if err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	defer rows.Close()
	var out []LineItem
	for rows.Next() {
		var idStr, pidStr, amtStr, qtyStr string
		it := LineItem{}
		if err := rows.Scan(&idStr, &pidStr, &amtStr, &qtyStr, &it.SnapshotRaw, &it.MetadataRaw); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if it.ID, err = uuid.Parse(idStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if it.ProductID, err = uuid.Parse(pidStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if it.Amount, err = decimal.NewFromString(amtStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if it.Quantity, err = decimal.NewFromString(qtyStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// ListBills 账单列表（可选客户过滤）。
func ListBills(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID, customerID *uuid.UUID, limit int) ([]*Bill, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	sql := `
SELECT id::text, customer_id::text, contract_id::text, status::text,
       period_start, period_end, total_amount::text, currency, finalized_at, created_at
FROM bills WHERE tenant_id=$1::uuid AND deleted_at IS NULL`
	args := []interface{}{tenantID.String()}
	if customerID != nil {
		sql += ` AND customer_id=$2::uuid`
		args = append(args, customerID.String())
	}
	sql += ` ORDER BY period_start DESC, created_at DESC LIMIT ` + fmt.Sprint(limit)

	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	defer rows.Close()
	var out []*Bill
	for rows.Next() {
		var idStr, cidStr, ctidStr, totalStr string
		b := &Bill{}
		if err := rows.Scan(&idStr, &cidStr, &ctidStr, &b.Status, &b.PeriodStart, &b.PeriodEnd,
			&totalStr, &b.Currency, &b.FinalizedAt, &b.CreatedAt); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if b.ID, err = uuid.Parse(idStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if b.CustomerID, err = uuid.Parse(cidStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if b.ContractID, err = uuid.Parse(ctidStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if b.TotalAmount, err = decimal.NewFromString(totalStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ListRuns 跑批任务列表。
func ListRuns(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID, limit int) ([]*BillingRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := db.Query(ctx, `
SELECT id::text, status::text, period_start, period_end,
       total_customers, total_bills, error_count, COALESCE(error_detail::text,''),
       started_at, completed_at
FROM billing_runs WHERE tenant_id=$1::uuid
ORDER BY created_at DESC LIMIT $2`,
		tenantID.String(), limit)
	if err != nil {
		return nil, common.ErrInternal.Wrap(err)
	}
	defer rows.Close()
	var out []*BillingRun
	for rows.Next() {
		var idStr string
		r := &BillingRun{}
		if err := rows.Scan(&idStr, &r.Status, &r.PeriodStart, &r.PeriodEnd,
			&r.TotalCustomers, &r.TotalBills, &r.ErrorCount, &r.ErrorDetail,
			&r.StartedAt, &r.CompletedAt); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		if r.ID, err = uuid.Parse(idStr); err != nil {
			return nil, common.ErrInternal.Wrap(err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
