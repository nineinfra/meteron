package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- customers ----

type customerView struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	ExternalID *string   `json:"external_id"`
	Currency   string    `json:"currency"`
	CreatedAt  time.Time `json:"created_at"`
}

func errBadRequest(msg string) error { return common.ErrBadRequest.Wrap(errors.New(msg)) }

func (s *Server) createCustomer(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		Name       string `json:"name"`
		ExternalID string `json:"external_id"`
		Currency   string `json:"currency"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeErr(w, errBadRequest("name required"))
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	var (
		idStr string
		extID *string
	)
	if req.ExternalID != "" {
		extID = &req.ExternalID
	}
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO customers (tenant_id, name, external_id, currency)
VALUES ($1::uuid, $2, $3, $4) RETURNING id::text`,
		ident.TenantID.String(), req.Name, extID, req.Currency).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}

func (s *Server) listCustomers(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	rows, err := s.db.Query(r.Context(), `
SELECT id::text, name, external_id, currency, created_at
FROM customers WHERE tenant_id=$1::uuid AND deleted_at IS NULL
ORDER BY created_at DESC LIMIT 100`, ident.TenantID.String())
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	defer rows.Close()
	out := []customerView{}
	for rows.Next() {
		var c customerView
		var idStr string
		if err := rows.Scan(&idStr, &c.Name, &c.ExternalID, &c.Currency, &c.CreatedAt); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
		if c.ID, err = uuid.Parse(idStr); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
		out = append(out, c)
	}
	writeOK(w, out)
}

func (s *Server) getCustomer(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var c customerView
	var idStr string
	err := s.db.QueryRow(r.Context(), `
SELECT id::text, name, external_id, currency, created_at
FROM customers WHERE id=$1::uuid AND tenant_id=$2::uuid AND deleted_at IS NULL`,
		id.String(), ident.TenantID.String()).Scan(&idStr, &c.Name, &c.ExternalID, &c.Currency, &c.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, common.ErrNotFound.Wrap(errors.New("customer")))
			return
		}
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	if c.ID, err = uuid.Parse(idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, c)
}

func (s *Server) updateCustomer(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Name     *string `json:"name"`
		Currency *string `json:"currency"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == nil && req.Currency == nil {
		writeErr(w, errBadRequest("nothing to update"))
		return
	}
	if req.Name != nil {
		if _, err := s.db.Exec(r.Context(),
			`UPDATE customers SET name=$3, updated_at=NOW() WHERE id=$1::uuid AND tenant_id=$2::uuid`,
			id.String(), ident.TenantID.String(), *req.Name); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
	}
	if req.Currency != nil {
		if _, err := s.db.Exec(r.Context(),
			`UPDATE customers SET currency=$3, updated_at=NOW() WHERE id=$1::uuid AND tenant_id=$2::uuid`,
			id.String(), ident.TenantID.String(), *req.Currency); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
	}
	writeOK(w, map[string]string{"id": id.String()})
}

// ---- 计量计价供给链 ----

func (s *Server) createBillableMetric(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		Name             string   `json:"name"`
		AggregationType  string   `json:"aggregation_type"`
		GroupKeys        []string `json:"group_keys"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeErr(w, errBadRequest("name required"))
		return
	}
	if req.AggregationType == "" {
		req.AggregationType = "sum"
	}
	if req.GroupKeys == nil {
		req.GroupKeys = []string{}
	}
	var idStr string
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO billable_metrics (tenant_id, name, aggregation_type, group_keys)
VALUES ($1::uuid, $2, $3, $4) RETURNING id::text`,
		ident.TenantID.String(), req.Name, req.AggregationType, req.GroupKeys).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}

func (s *Server) createProduct(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		MetricID    string `json:"metric_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == "" || req.MetricID == "" {
		writeErr(w, errBadRequest("name and metric_id required"))
		return
	}
	if req.DisplayName == "" {
		req.DisplayName = req.Name
	}
	metricID, err := uuid.Parse(req.MetricID)
	if err != nil {
		writeErr(w, errBadRequest("invalid metric_id"))
		return
	}
	var idStr string
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO products (tenant_id, metric_id, name, display_name)
VALUES ($1::uuid, $2::uuid, $3, $4) RETURNING id::text`,
		ident.TenantID.String(), metricID.String(), req.Name, req.DisplayName).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}

func (s *Server) createRateCard(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeErr(w, errBadRequest("name required"))
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	var idStr string
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO rate_cards (tenant_id, name, currency, valid_from)
VALUES ($1::uuid, $2, $3, NOW()) RETURNING id::text`,
		ident.TenantID.String(), req.Name, req.Currency).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}

func (s *Server) createRateRule(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	cardID, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		ProductID       string            `json:"product_id"`
		MatchConditions map[string]string `json:"match_conditions"`
		PricingType     string            `json:"pricing_type"`
		PricingConfig   json.RawMessage   `json:"pricing_config"`
		Priority        int               `json:"priority"`
		IsDefaultRule   bool              `json:"is_default_rule"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	productID, err := uuid.Parse(req.ProductID)
	if err != nil {
		writeErr(w, errBadRequest("invalid product_id"))
		return
	}
	if req.PricingType != "flat" && req.PricingType != "tiered" {
		writeErr(w, errBadRequest("pricing_type must be flat|tiered"))
		return
	}
	if req.MatchConditions == nil {
		req.MatchConditions = map[string]string{}
	}
	// 校验定价配置合法性
	var cfg pricing.PricingConfig
	if err := json.Unmarshal(req.PricingConfig, &cfg); err != nil {
		writeErr(w, errBadRequest("invalid pricing_config"))
		return
	}
	if err := pricing.ValidateConfig(req.PricingType, &cfg); err != nil {
		writeErr(w, errBadRequest(err.Error()))
		return
	}
	var idStr string
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO rate_card_rules (rate_card_id, product_id, match_conditions, pricing_type, pricing_config, priority, is_default_rule)
VALUES ($1::uuid, $2::uuid, $3::jsonb, $4, $5::jsonb, $6, $7) RETURNING id::text`,
		cardID.String(), productID.String(), req.MatchConditions, req.PricingType,
		string(req.PricingConfig), req.Priority, req.IsDefaultRule).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}

func (s *Server) createContract(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		CustomerID string  `json:"customer_id"`
		RateCardID string  `json:"rate_card_id"`
		StartTime  string  `json:"start_time"`
		EndTime    *string `json:"end_time"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	customerID, err := uuid.Parse(req.CustomerID)
	if err != nil {
		writeErr(w, errBadRequest("invalid customer_id"))
		return
	}
	rateCardID, err := uuid.Parse(req.RateCardID)
	if err != nil {
		writeErr(w, errBadRequest("invalid rate_card_id"))
		return
	}
	var startTime time.Time
	if req.StartTime != "" {
		startTime, err = time.Parse(time.RFC3339, req.StartTime)
		if err != nil {
			writeErr(w, errBadRequest("invalid start_time"))
			return
		}
	} else {
		startTime = time.Now().UTC()
	}
	var endTime *time.Time
	if req.EndTime != nil && *req.EndTime != "" {
		t, err := time.Parse(time.RFC3339, *req.EndTime)
		if err != nil {
			writeErr(w, errBadRequest("invalid end_time"))
			return
		}
		endTime = &t
	}
	var idStr string
	if err := s.db.QueryRow(r.Context(), `
INSERT INTO contracts (tenant_id, customer_id, rate_card_id, start_time, end_time)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5) RETURNING id::text`,
		ident.TenantID.String(), customerID.String(), rateCardID.String(), startTime, endTime).Scan(&idStr); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"id": idStr})
}
