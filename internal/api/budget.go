package api

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- budget pools ----

func (s *Server) createBudgetPool(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		CustomerID       string  `json:"customer_id"`
		TaskType         *string `json:"task_type"`
		AgentID          *string `json:"agent_id"`
		MaxBudget        string  `json:"max_budget"`
		WarningThreshold *string `json:"warning_threshold"`
		ExhaustPolicy    string  `json:"exhaust_policy"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	customerID, err := uuid.Parse(req.CustomerID)
	if err != nil {
		writeErr(w, errBadRequest("invalid customer_id"))
		return
	}
	maxBudget, err := decimal.NewFromString(req.MaxBudget)
	if err != nil || maxBudget.IsNegative() {
		writeErr(w, errBadRequest("invalid max_budget"))
		return
	}
	switch req.ExhaustPolicy {
	case "warn", "block", "fallback":
	default:
		writeErr(w, errBadRequest("exhaust_policy must be warn|block|fallback"))
		return
	}
	var warnThreshold *string
	if req.WarningThreshold != nil && *req.WarningThreshold != "" {
		if _, err := decimal.NewFromString(*req.WarningThreshold); err != nil {
			writeErr(w, errBadRequest("invalid warning_threshold"))
			return
		}
		warnThreshold = req.WarningThreshold
	}
	var taskType, agentID *string
	if req.TaskType != nil && *req.TaskType != "" {
		taskType = req.TaskType
	}
	if req.AgentID != nil && *req.AgentID != "" {
		agentID = req.AgentID
	}

	var idStr string
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO budget_pools (tenant_id, customer_id, task_type, agent_id, max_budget, warning_threshold, exhaust_policy)
VALUES ($1::uuid, $2::uuid, $3, $4, $5::numeric, $6::numeric, $7) RETURNING id::text`,
		ident.TenantID.String(), customerID.String(), taskType, agentID,
		maxBudget.String(), warnThreshold, req.ExhaustPolicy).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}

// poolBalance 查询预算池余额（PG 可信源实时计算 + Redis 缓存值参考）。
func (s *Server) poolBalance(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	poolID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	pool, err := s.ledgerSvc.Store().GetPool(r.Context(), ident.TenantID, poolID)
	if err != nil {
		writeErr(w, err)
		return
	}
	spent, pending, err := s.ledgerSvc.Store().SpentAndPending(r.Context(), poolID)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	avail := pool.MaxBudget.Sub(spent).Sub(pending)
	cacheAvail, cacheExists, err := s.ledgerSvc.CacheAvail(r.Context(), poolID)
	if err != nil {
		cacheExists = false
		s.log.Warn("read budget cache failed", "pool_id", poolID, "err", err)
	}
	bal := ledger.PoolBalance{
		PoolID:          pool.ID,
		MaxBudget:       pool.MaxBudget,
		SpentAmount:     spent,
		PendingReserved: pending,
		AvailableAmount: avail,
	}
	if cacheExists {
		bal.CacheAvailMicro = &cacheAvail
	}
	writeOK(w, bal)
}

// ---- reservations ----

func (s *Server) confirmReservation(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	resID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		ActualAmount string `json:"actual_amount"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	actual, err := decimal.NewFromString(req.ActualAmount)
	if err != nil || actual.IsNegative() {
		writeErr(w, errBadRequest("invalid actual_amount"))
		return
	}
	res, err := s.ledgerSvc.Confirm(r.Context(), ident.TenantID, resID, actual)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (s *Server) releaseReservation(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	resID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	res, err := s.ledgerSvc.Release(r.Context(), ident.TenantID, resID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}
