// Package api Management API（/api/v1）：租户资源供给、用量查询、账单管理。
// 鉴权：Authorization: Bearer <api-key>；scope 控制（admin 具备全部权限）。
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/meteron/meteron/internal/billing"
	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/rbac"
)

const maxAPIBody = 1 << 20 // 1MB

// Server Management API 服务。
type Server struct {
	db        *pgxpool.Pool
	ch        driver.Conn
	auth      *rbac.Service
	ledgerSvc *ledger.Service
	pricing   *pricing.Engine
	producer  *ingest.Producer
	billing   *billing.Worker
	cfg       *config.Config
	log       *slog.Logger
}

func NewServer(
	db *pgxpool.Pool,
	ch driver.Conn,
	auth *rbac.Service,
	ledgerSvc *ledger.Service,
	pricingEngine *pricing.Engine,
	producer *ingest.Producer,
	billingWorker *billing.Worker,
	cfg *config.Config,
	log *slog.Logger,
) *Server {
	return &Server{
		db: db, ch: ch, auth: auth, ledgerSvc: ledgerSvc,
		pricing: pricingEngine, producer: producer, billing: billingWorker,
		cfg: cfg, log: log,
	}
}

// Handler 构建 /api/v1 路由（Go 1.22+ method+wildcard 模式）。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// customers
	mux.HandleFunc("GET /api/v1/customers", s.listCustomers)
	mux.HandleFunc("POST /api/v1/customers", s.createCustomer)
	mux.HandleFunc("GET /api/v1/customers/{id}", s.getCustomer)
	mux.HandleFunc("PATCH /api/v1/customers/{id}", s.updateCustomer)

	// 计量计价供给链（products / rate-cards / rules / contracts）
	mux.HandleFunc("POST /api/v1/billable-metrics", s.createBillableMetric)
	mux.HandleFunc("POST /api/v1/products", s.createProduct)
	mux.HandleFunc("POST /api/v1/rate-cards", s.createRateCard)
	mux.HandleFunc("POST /api/v1/rate-cards/{id}/rules", s.createRateRule)
	mux.HandleFunc("POST /api/v1/contracts", s.createContract)

	// 预算
	mux.HandleFunc("POST /api/v1/budget-pools", s.createBudgetPool)
	mux.HandleFunc("GET /api/v1/budget-pools/{id}/balance", s.poolBalance)
	mux.HandleFunc("POST /api/v1/budget-reservations/{id}/confirm", s.confirmReservation)
	mux.HandleFunc("POST /api/v1/budget-reservations/{id}/release", s.releaseReservation)

	// 事件上报与用量
	mux.HandleFunc("POST /api/v1/ingest/events", s.ingestEvent)
	mux.HandleFunc("GET /api/v1/usage", s.usage)
	mux.HandleFunc("GET /api/v1/usage/timeseries", s.usageTimeseries)

	// 账单与跑批
	mux.HandleFunc("GET /api/v1/bills", s.listBills)
	mux.HandleFunc("GET /api/v1/bills/{id}", s.getBill)
	mux.HandleFunc("POST /api/v1/bills/{id}/finalize", s.finalizeBill)
	mux.HandleFunc("POST /api/v1/bills/{id}/amend", s.amendBill)
	mux.HandleFunc("GET /api/v1/billing-runs", s.listBillingRuns)
	mux.HandleFunc("POST /api/v1/billing-runs", s.triggerBillingRun)
	mux.HandleFunc("GET /api/v1/reconciliation-runs", s.listReconciliationRuns)

	return s.authMiddleware(mux)
}

// authMiddleware Bearer 鉴权，身份注入请求上下文。
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		if token == "" {
			writeErr(w, common.ErrUnauthorized.Wrap(errors.New("missing bearer token")))
			return
		}
		ident, err := s.auth.Authenticate(r.Context(), token)
		if err != nil {
			writeErr(w, common.ErrUnauthorized.Wrap(err))
			return
		}
		next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), ident)))
	})
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	parts := strings.SplitN(h, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

// requireScope scope 校验（admin 隐含全部）。
func requireScope(w http.ResponseWriter, r *http.Request, scope string) *rbac.Identity {
	ident := identityOf(r)
	if ident == nil {
		writeErr(w, common.ErrUnauthorized.Wrap(errors.New("no identity")))
		return nil
	}
	if !ident.HasScope(scope) {
		writeErr(w, common.ErrUnauthorized.Wrap(errors.New("missing scope "+scope)))
		return nil
	}
	return ident
}

// pathID 解析路径参数为 UUID。
func pathID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(errors.New("invalid "+name)))
		return uuid.Nil, false
	}
	return id, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAPIBody))
	if err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(err))
		return false
	}
	if err := json.Unmarshal(body, v); err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(errors.New("invalid json body")))
		return false
	}
	return true
}

func writeErr(w http.ResponseWriter, err error) {
	biz := common.FromErr(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(biz.HTTPStatus)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code": biz.Code,
		"msg":  biz.Msg,
		"data": map[string]string{"detail": err.Error()},
	})
}

func writeOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code": common.CodeSuccess,
		"msg":  "OK",
		"data": data,
	})
}
