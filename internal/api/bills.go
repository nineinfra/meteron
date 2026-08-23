package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/meteron/meteron/internal/billing"
	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- bills ----

func (s *Server) listBills(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	customerID, err := customerFilter(r, ident)
	if err != nil {
		writeErr(w, err)
		return
	}
	bills, err := billing.ListBills(r.Context(), s.db, ident.TenantID, customerID, 100)
	if err != nil {
		writeErr(w, err)
		return
	}
	if bills == nil {
		bills = []*billing.Bill{}
	}
	writeOK(w, bills)
}

func (s *Server) getBill(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	billID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	b, err := billing.GetBill(r.Context(), s.db, ident.TenantID, billID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, b)
}

func (s *Server) finalizeBill(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	billID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = decodeBodySilent(r, &req) // body 可选：空/非法 body 静默忽略

	b, err := billing.FinalizeBill(r.Context(), s.db, ident.TenantID, billID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, b)
}

// amendBill 账单修正（amending）：V1 完整实现；MVP 未实现。
func (s *Server) amendBill(w http.ResponseWriter, r *http.Request) {
	_ = requireScope(w, r, rbac.ScopeAdmin)
	writeErr(w, common.ErrNotImplemented.Wrap(errors.New("amending is V1 scope")))
}

// ---- billing runs ----

func (s *Server) listBillingRuns(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	runs, err := billing.ListRuns(r.Context(), s.db, ident.TenantID, 100)
	if err != nil {
		writeErr(w, err)
		return
	}
	if runs == nil {
		runs = []*billing.BillingRun{}
	}
	writeOK(w, runs)
}

// triggerBillingRun 手动触发账单跑批（period 缺省为上一自然日 UTC）。
func (s *Server) triggerBillingRun(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		PeriodStart string `json:"period_start"`
		PeriodEnd   string `json:"period_end"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	var from, to time.Time
	var err error
	if req.PeriodStart == "" {
		now := time.Now().UTC()
		from = now.AddDate(0, 0, -1).Truncate(24 * time.Hour)
		to = from.Add(24 * time.Hour)
	} else {
		from, err = time.Parse(time.RFC3339, req.PeriodStart)
		if err != nil {
			writeErr(w, errBadRequest("invalid period_start"))
			return
		}
		if req.PeriodEnd == "" {
			to = from.Add(24 * time.Hour)
		} else {
			to, err = time.Parse(time.RFC3339, req.PeriodEnd)
			if err != nil {
				writeErr(w, errBadRequest("invalid period_end"))
				return
			}
		}
	}
	runID, err := s.billing.RunBilling(r.Context(), ident.TenantID, from.UTC(), to.UTC())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]interface{}{
		"run_id":       runID,
		"period_start": from.UTC(),
		"period_end":   to.UTC(),
	})
}

// ---- reconciliation runs（只读查询；Worker 由任务 #12 实现）----

func (s *Server) listReconciliationRuns(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	rows, err := s.db.Query(r.Context(), `
SELECT id::text, run_date, status::text, scope::text,
       usage_total::text, rated_total::text, ledger_total::text, diff_amount::text,
       corrected_count, started_at, completed_at
FROM reconciliation_runs WHERE tenant_id=$1::uuid
ORDER BY created_at DESC LIMIT 100`, ident.TenantID.String())
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	defer rows.Close()
	type runView struct {
		ID            string  `json:"id"`
		RunDate       string  `json:"run_date"`
		Status        string  `json:"status"`
		Scope         string  `json:"scope"`
		UsageTotal    *string `json:"usage_total"`
		RatedTotal    *string `json:"rated_total"`
		LedgerTotal   *string `json:"ledger_total"`
		DiffAmount    *string `json:"diff_amount"`
		CorrectedCount int    `json:"corrected_count"`
		StartedAt     *time.Time `json:"started_at"`
		CompletedAt   *time.Time `json:"completed_at"`
	}
	out := []runView{}
	for rows.Next() {
		var v runView
		if err := rows.Scan(&v.ID, &v.RunDate, &v.Status, &v.Scope,
			&v.UsageTotal, &v.RatedTotal, &v.LedgerTotal, &v.DiffAmount,
			&v.CorrectedCount, &v.StartedAt, &v.CompletedAt); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
		out = append(out, v)
	}
	writeOK(w, out)
}
