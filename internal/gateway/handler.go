// Package gateway 在线代理网关：OpenAI 兼容转发、预算预留、策略路由、SSE 流式。
//
// 分层约束（详细设计文档 §1）：在线链路只做轻量预算预留校验；
// 完整计价、账本持久化、出账全部异步后台完成。
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/metrics"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/provider"
	"github.com/meteron/meteron/internal/rbac"
	"github.com/meteron/meteron/internal/router"
)

const (
	headerCustomerID = "X-Meteron-Customer-ID"
	headerTaskType   = "X-Meteron-Task-Type"
	headerAgentID    = "X-Meteron-Agent-ID"
	headerIdemKey    = "X-Idempotency-Key"

	maxBodyBytes = 10 << 20 // 10MB
)

// idemNamespace X-Idempotency-Key → transaction_id 的 UUIDv5 命名空间。
var idemNamespace = uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8") // RFC4122 URL namespace

type Handler struct {
	auth      *rbac.Service
	ledgerSvc *ledger.Service
	ledgerSt  *ledger.Store
	router    *router.Engine
	pricing   *pricing.Engine
	providers *provider.Registry
	producer  *ingest.Producer
	db        *pgxpool.Pool
	cfg       *config.Config
	log       *slog.Logger
}

func NewHandler(
	auth *rbac.Service,
	ledgerSvc *ledger.Service,
	ledgerSt *ledger.Store,
	routerEngine *router.Engine,
	pricingEngine *pricing.Engine,
	providers *provider.Registry,
	producer *ingest.Producer,
	db *pgxpool.Pool,
	cfg *config.Config,
	log *slog.Logger,
) *Handler {
	return &Handler{
		auth: auth, ledgerSvc: ledgerSvc, ledgerSt: ledgerSt,
		router: routerEngine, pricing: pricingEngine,
		providers: providers, producer: producer, db: db,
		cfg: cfg, log: log,
	}
}

// ServeHTTP POST /v1/chat/completions
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()

	// 1. 鉴权
	ident, err := h.authenticate(r)
	if err != nil {
		writeErr(w, common.ErrUnauthorized.Wrap(err))
		return
	}

	// 2. 解析元数据
	customerIDStr := r.Header.Get(headerCustomerID)
	if customerIDStr == "" {
		writeErr(w, common.ErrBadRequest.Wrap(errors.New("missing "+headerCustomerID)))
		return
	}
	taskType := r.Header.Get(headerTaskType)
	agentID := r.Header.Get(headerAgentID)
	idemKey := r.Header.Get(headerIdemKey)

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(err))
		return
	}
	var chatReq chatRequest
	if err := json.Unmarshal(body, &chatReq); err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(errors.New("invalid chat request body")))
		return
	}
	if len(chatReq.Messages) == 0 {
		writeErr(w, common.ErrBadRequest.Wrap(errors.New("messages required")))
		return
	}

	// 3. 客户解析（UUID 或 external_id，租户隔离）
	customerID, err := h.resolveCustomer(ctx, ident.TenantID, customerIDStr)
	if err != nil {
		writeErr(w, err)
		return
	}

	// 4. transaction_id：优先映射 X-Idempotency-Key（UUIDv5），否则新建
	var txnID uuid.UUID
	if idemKey != "" {
		txnID = uuid.NewSHA1(idemNamespace, []byte(idemKey))
	} else {
		txnID = uuid.New()
	}

	// 幂等：同租户同 transaction_id 已存在预留 → 拒绝重复提交（防重复计费）
	if idemKey != "" {
		if err := h.checkTransactionExists(ctx, ident.TenantID, txnID); err != nil {
			writeErr(w, err)
			return
		}
	}

	// 5. 预算池匹配 + 路由决策
	pool, err := h.ledgerSt.FindPool(ctx, ident.TenantID, customerID, taskType, agentID)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	availRatio := 1.0
	if pool != nil {
		availRatio = h.poolAvailRatio(ctx, pool)
	}

	route, err := h.router.Resolve(ctx, ident.TenantID, taskType, chatReq.Model, availRatio)
	if err != nil {
		writeErr(w, common.FromErr(err))
		return
	}
	// 无路由规则时按模型反查 provider
	if route.Provider == "" {
		pName, ok := h.providers.ProviderForModel(route.Model)
		if !ok {
			writeErr(w, common.ErrNoProvider.Wrap(fmt.Errorf("model %q", route.Model)))
			return
		}
		route.Provider = pName
	}

	// 6. 预算预留（reserve）；预算不足时执行预设策略（告警放行 / 拒绝 / 降级廉价模型）
	ttl := time.Duration(h.cfg.Business.ReserveDefaultTTLSeconds) * time.Second
	var reservation *ledger.Reservation
	if pool != nil {
		estAmount := h.estimateAmount(&chatReq)
		reservation, err = h.ledgerSvc.Reserve(ctx, pool, estAmount, txnID, ttl)
		if err != nil {
			biz := common.FromErr(err)
			if biz.Code != common.CodeBudgetExhausted {
				writeErr(w, biz)
				return
			}
			switch pool.ExhaustPolicy {
			case "warn":
				// 告警放行：无预留继续调用（超支通过指标与对账暴露）
				h.log.Warn("budget exhausted, policy=warn, proceed without reservation",
					"pool_id", pool.ID, "customer_id", pool.CustomerID)
				reservation = nil
			case "fallback":
				// 自动降级路由廉价模型：半价预估重试预留
				if route.FallbackProvider == "" || route.FallbackModel == "" ||
					(route.FallbackProvider == route.Provider && route.FallbackModel == route.Model) {
					writeErr(w, common.ErrBudgetExhausted.Wrap(errors.New("no fallback model configured")))
					return
				}
				reservation, err = h.ledgerSvc.Reserve(ctx, pool, estAmount.Div(decimal.NewFromInt(2)), txnID, ttl)
				if err != nil {
					writeErr(w, common.FromErr(err))
					return
				}
				h.log.Warn("budget exhausted, fallback to cheaper model",
					"pool_id", pool.ID, "from", route.Model, "to", route.FallbackModel)
				route.Provider, route.Model = route.FallbackProvider, route.FallbackModel
			default: // block
				writeErr(w, common.ErrBudgetExhausted)
				return
			}
		}
	}

	// 7. Provider 适配器（fallback 可能已切换 provider）
	adapter, ok := h.providers.Get(route.Provider)
	if !ok {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
		}
		writeErr(w, common.ErrNoProvider.Wrap(fmt.Errorf("provider %q", route.Provider)))
		return
	}

	// 8. 计价上下文快照（请求匹配成功时刻固化）
	dims := pricing.Dims{TaskType: taskType, Model: route.Model, Provider: route.Provider, AgentID: agentID}
	snapshot, err := h.pricing.ResolveSnapshot(ctx, ident.TenantID, customerID, dims)
	if err != nil {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
		}
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}

	// 9. 转发 LLM
	w.Header().Set("X-Meteron-Transaction-ID", txnID.String())
	if reservation != nil {
		w.Header().Set("X-Meteron-Reservation-ID", reservation.ID.String())
	}

	req := &provider.ChatRequest{Model: route.Model, Body: body, Stream: chatReq.Stream}
	result, err := adapter.Chat(ctx, req)
	if err != nil {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID) // 未产生 usage：全额释放
		}
		writeErr(w, common.ErrUpstream.Wrap(err))
		return
	}
	if result.StatusCode >= 400 {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
		}
		w.Header().Set("Content-Type", result.ContentType)
		w.WriteHeader(result.StatusCode)
		_, _ = w.Write(result.Body)
		return
	}

	finish := func(usage *provider.Usage) {
		// confirm + release 差额 + 构造 MeterEvent 投递 Kafka（全链路异步化的起点）
		if usage == nil {
			return // SSE 中途断开：不 confirm、不计费，等待 ScheduledWorker 过期释放
		}
		// 客户端断开（SSE 写失败）会取消 r.Context()，但 LLM 成本已实际发生：
		// 必须脱离请求生命周期完成 confirm 与事件投递，否则预算被错误归还（超卖）。
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		actual := pricing.EventCost(snapshot, usage.InputTokens, usage.OutputTokens, usage.CachedInputTokens)
		if reservation != nil {
			if _, cerr := h.ledgerSvc.Confirm(fctx, ident.TenantID, reservation.ID, actual); cerr != nil {
				// 预留已过期（如流式时长超过 TTL）：预算已归还，但计量仍需如实上报
				h.log.Warn("reservation confirm failed, event still emitted",
					"reservation_id", reservation.ID, "err", cerr)
			}
		}
		ev := h.buildEvent(ident.TenantID, customerID, txnID, route, taskType, agentID, snapshot, usage, body, reservation, idemKey, chatReq.Model)
		if perr := h.producer.Produce(fctx, ev); perr != nil {
			h.log.Error("produce meter event failed", "event_id", ev.EventID, "err", perr)
		}
	}

	if chatReq.Stream {
		h.proxyStream(w, result.Stream, finish)
	} else {
		w.Header().Set("Content-Type", result.ContentType)
		w.WriteHeader(result.StatusCode)
		_, _ = w.Write(result.Body)
		finish(result.Usage)
	}
	metrics.GatewayLatencySeconds.Observe(time.Since(start).Seconds())
}

// proxyStream SSE 透传 + usage 嗅探。
// ① 流完整结束且拿到 usage → onFinish(confirm+event)
// ② SSE 中途断开/客户端断开/无 usage → 不 confirm，等待 ScheduledWorker expire 归还全部预留
func (h *Handler) proxyStream(w http.ResponseWriter, stream io.ReadCloser, onFinish func(*provider.Usage)) {
	defer stream.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	sniffer := newUsageSniffer(stream)
	buf := make([]byte, 32*1024)
	for {
		n, rerr := sniffer.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				// 客户端断开：若已捕获 usage 仍然计费（LLM 已产生成本）
				onFinish(sniffer.Usage())
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			// EOF=正常结束；其他=上游中断。有 usage 则计费，无 usage 则走过期释放
			onFinish(sniffer.Usage())
			return
		}
	}
}

// ---- helpers ----

type chatRequest struct {
	Model       string          `json:"model"`
	Messages    []chatMessage   `json:"messages"`
	MaxTokens   *int            `json:"max_tokens"`
	Stream      bool            `json:"stream"`
}

type chatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

func (m chatMessage) textLen() int {
	var s string
	if err := json.Unmarshal(m.Content, &s); err == nil {
		return len(s)
	}
	var parts []struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(m.Content, &parts); err == nil {
		n := 0
		for _, p := range parts {
			n += len(p.Text)
		}
		return n
	}
	return 0
}

func (h *Handler) estimateAmount(req *chatRequest) decimal.Decimal {
	chars := 0
	for _, m := range req.Messages {
		chars += m.textLen()
	}
	cpt := h.cfg.Business.EstimateCharsPerToken
	if cpt <= 0 {
		cpt = 4
	}
	estTokens := uint64(chars/cpt + 1)
	maxTok := h.cfg.Business.EstimateDefaultMaxTokens
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		maxTok = *req.MaxTokens
	}
	estTokens += uint64(maxTok)
	price, err := decimal.NewFromString(h.cfg.Business.EstimateUnitPrice)
	if err != nil {
		price = decimal.Zero
	}
	return price.Mul(decimal.NewFromInt(int64(estTokens)))
}

func (h *Handler) poolAvailRatio(ctx context.Context, pool *ledger.Pool) float64 {
	spent, pending, err := h.ledgerSt.SpentAndPending(ctx, pool.ID)
	if err != nil {
		return 1
	}
	avail := pool.MaxBudget.Sub(spent).Sub(pending)
	if pool.MaxBudget.IsZero() {
		return 0
	}
	ratio, _ := avail.Div(pool.MaxBudget).Float64()
	if ratio < 0 {
		return 0
	}
	return ratio
}

func (h *Handler) resolveCustomer(ctx context.Context, tenantID uuid.UUID, ref string) (uuid.UUID, error) {
	var idStr string
	if _, err := uuid.Parse(ref); err == nil {
		err := h.db.QueryRow(ctx,
			`SELECT id::text FROM customers WHERE tenant_id=$1::uuid AND id=$2::uuid AND deleted_at IS NULL`,
			tenantID.String(), ref).Scan(&idStr)
		if err == nil {
			return uuid.Parse(idStr)
		}
		if err != pgx.ErrNoRows {
			return uuid.Nil, common.ErrInternal.Wrap(err)
		}
	}
	err := h.db.QueryRow(ctx,
		`SELECT id::text FROM customers WHERE tenant_id=$1::uuid AND external_id=$2 AND deleted_at IS NULL`,
		tenantID.String(), ref).Scan(&idStr)
	if err != nil {
		if err == pgx.ErrNoRows {
			return uuid.Nil, common.ErrNotFound.Wrap(fmt.Errorf("customer %q", ref))
		}
		return uuid.Nil, common.ErrInternal.Wrap(err)
	}
	return uuid.Parse(idStr)
}

func (h *Handler) checkTransactionExists(ctx context.Context, tenantID, txnID uuid.UUID) error {
	var exists bool
	err := h.db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM budget_reservations WHERE tenant_id=$1::uuid AND transaction_id=$2::uuid)`,
		tenantID.String(), txnID.String()).Scan(&exists)
	if err != nil {
		return common.ErrInternal.Wrap(err)
	}
	if exists {
		return common.ErrDuplicateTransaction
	}
	return nil
}

func (h *Handler) buildEvent(
	tenantID, customerID, txnID uuid.UUID,
	route *router.RouteResult, taskType, agentID string,
	snapshot *pricing.Snapshot, usage *provider.Usage,
	rawBody []byte, reservation *ledger.Reservation, idemKey, requestedModel string,
) *ingest.MeterEvent {
	providerName := route.Provider
	modelName := route.Model
	eventType := "llm_request"
	metadata := map[string]string{
		"requested_model": requestedModel,
	}
	if idemKey != "" {
		metadata["idempotency_key"] = idemKey
	}
	if reservation != nil {
		metadata["reservation_id"] = reservation.ID.String()
	}
	if route.RuleID != nil {
		metadata["route_rule_id"] = route.RuleID.String()
	}
	currency := "USD"
	if snapshot != nil && snapshot.Currency != "" {
		currency = snapshot.Currency
	}
	raw := string(rawBody)
	if len(raw) > 64*1024 {
		raw = raw[:64*1024]
	}
	return &ingest.MeterEvent{
		EventID:                uuid.New(),
		TransactionID:          txnID,
		EventVersion:           1,
		TenantID:               tenantID,
		CustomerID:             customerID,
		EventType:              eventType,
		Timestamp:              time.Now().UTC(),
		Provider:               providerName,
		Model:                  modelName,
		TaskType:               taskType,
		AgentID:                agentID,
		InputTokens:            usage.InputTokens,
		OutputTokens:           usage.OutputTokens,
		CachedInputTokens:      usage.CachedInputTokens,
		PricingContextSnapshot: snapshot,
		Currency:               currency,
		Metadata:               metadata,
		RawPayload:             raw,
	}
}

func (h *Handler) authenticate(r *http.Request) (*rbac.Identity, error) {
	authz := r.Header.Get("Authorization")
	token := strings.TrimPrefix(authz, "Bearer ")
	if token == "" || token == authz {
		return nil, errors.New("missing bearer token")
	}
	ident, err := h.auth.Authenticate(r.Context(), token)
	if err != nil {
		return nil, err
	}
	if !ident.HasScope(rbac.ScopeGateway) {
		return nil, errors.New("api key missing gateway_invoke scope")
	}
	return ident, nil
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
