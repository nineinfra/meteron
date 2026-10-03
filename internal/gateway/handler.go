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
	"github.com/meteron/meteron/internal/health"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/metrics"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/provider"
	"github.com/meteron/meteron/internal/ratelimit"
	"github.com/meteron/meteron/internal/rbac"
	"github.com/meteron/meteron/internal/router"
)

const (
	headerCustomerID = "X-Meteron-Customer-ID"
	headerTaskType   = "X-Meteron-Task-Type"
	headerAgentID    = "X-Meteron-Agent-ID"
	headerIdemKey    = "X-Idempotency-Key"
	// 应用侧渠道选择：显式锁定上游渠道（跳过路由规则）；
	// 质量要求（auto 模式成本路由信号）：low/medium/high → economy/standard/premium
	headerProvider = "X-Meteron-Provider"
	headerQuality  = "X-Meteron-Quality"
	modelAuto      = "auto"

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
	health    *health.Tracker   // 渠道健康（nil=未启用）
	limiter   ratelimit.Limiter // 双重限流（nil=未启用）
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
	healthTracker *health.Tracker,
	limiter ratelimit.Limiter,
) *Handler {
	return &Handler{
		auth: auth, ledgerSvc: ledgerSvc, ledgerSt: ledgerSt,
		router: routerEngine, pricing: pricingEngine,
		providers: providers, producer: producer, db: db,
		cfg: cfg, log: log,
		health: healthTracker, limiter: limiter,
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

	// 1.5 密钥 IP 白名单（空名单不限制；fail-closed：有名单但来源 IP 不可解析则拒绝）
	var trustedProxies []string
	if h.cfg != nil {
		trustedProxies = h.cfg.Security.TrustedProxies
	}
	if !rbac.IPAllowed(ident.IPWhitelist, clientIP(r, trustedProxies)) {
		writeErr(w, common.ErrIPNotAllowed.Wrap(errors.New("source ip not in key whitelist")))
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
	if err := validateChatRequest(&chatReq); err != nil {
		writeErr(w, common.ErrBadRequest.Wrap(err))
		return
	}

	// 3. 客户解析（UUID 或 external_id，租户隔离）
	customerID, err := h.resolveCustomer(ctx, ident.TenantID, customerIDStr)
	if err != nil {
		writeErr(w, err)
		return
	}
	// 3.1 密钥客户绑定：客户级密钥只能以绑定客户身份调用
	// （防持有客户 A 密钥者把用量/预算/账单记到同租户客户 B 头上，越权计费）
	if !customerAllowed(ident.CustomerID, customerID) {
		writeErr(w, common.ErrCustomerNotAllowed.Wrap(errors.New("key bound to another customer")))
		return
	}

	// 3.5 客户维度限流：RPM（cost=请求）/ TPM（cost=估算 token）/ 最大并发。
	// 保护性限流 fail-open：Redis 故障时放行并告警（区别于预算的 fail-closed）。
	if release, ok := h.applyCustomerRateLimits(w, ctx, ident.TenantID, customerID, &chatReq); !ok {
		return // writeErr 已在内部完成
	} else if release != nil {
		defer release()
	}

	// 4. transaction_id：优先映射 X-Idempotency-Key（UUIDv5），否则新建
	var txnID uuid.UUID
	if idemKey != "" {
		txnID = uuid.NewSHA1(idemNamespace, []byte(idemKey))
	} else {
		txnID = uuid.New()
	}

	// 幂等：同租户同 transaction_id 已占用 → 拒绝重复提交（防重复计费）。
	// 占用独立落表 idempotency_keys（ON CONFLICT DO NOTHING 原子占用）：
	// 旧实现依赖 budget_reservations 唯一约束，客户无预算池（不建预留）时
	// 同键重放会产生两条计量事件（双倍计费）。
	if idemKey != "" {
		if err := h.occupyIdempotencyKey(ctx, ident.TenantID, txnID); err != nil {
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

	// 5.5 路由决策（三模式）：
	//   a. 显式 X-Meteron-Provider：应用锁定渠道（跳过路由规则；model=auto 时取该渠道默认模型）
	//   b. model=auto：Meteron 按请求复杂度自动路由（成本分级策略，见 router.ResolveAuto）
	//   c. 具体模型：现有 route_rules / 模型反查
	requestedProvider := r.Header.Get(headerProvider)
	isAuto := strings.EqualFold(chatReq.Model, modelAuto)
	var route *router.RouteResult
	if requestedProvider != "" {
		if _, ok := h.providers.Get(requestedProvider); !ok {
			writeErr(w, common.ErrBadRequest.Wrap(fmt.Errorf("unknown provider %q", requestedProvider)))
			return
		}
		model := chatReq.Model
		if isAuto {
			m, ok := h.providers.DefaultModelForProvider(requestedProvider)
			if !ok {
				writeErr(w, common.ErrBadRequest.Wrap(fmt.Errorf("provider %q has no models", requestedProvider)))
				return
			}
			model = m
		}
		route = &router.RouteResult{Provider: requestedProvider, Model: model}
	} else if isAuto {
		route, err = h.router.ResolveAuto(ctx, ident.TenantID, taskType, h.buildProfile(r, &chatReq), availRatio)
		if err != nil {
			writeErr(w, common.FromErr(err))
			return
		}
	} else {
		route, err = h.router.Resolve(ctx, ident.TenantID, taskType, chatReq.Model, availRatio)
		if err != nil {
			writeErr(w, common.FromErr(err))
			return
		}
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

	// 5.8 计价上下文快照（请求匹配成功时刻固化，前置于预算预留）：
	// 预留金额按快照单价估算（与实际计费同口径），修复全局估价与真实费率
	// 脱钩导致高价模型单笔远超预留的超卖窗口。路由/降级切换模型处同步重取。
	dims := pricing.Dims{TaskType: taskType, Model: route.Model, Provider: route.Provider, AgentID: agentID}
	snapshot, err := h.pricing.ResolveSnapshot(ctx, ident.TenantID, customerID, dims)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}

	// 6. 预算预留（reserve）；预算不足时执行预设策略（告警放行 / 拒绝 / 降级廉价模型）
	ttl := time.Duration(h.cfg.Business.ReserveDefaultTTLSeconds) * time.Second
	var reservation *ledger.Reservation
	if pool != nil {
		estAmount := h.estimateAmount(snapshot, &chatReq)
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
				// 自动降级路由廉价模型：按 fallback 模型费率重取快照并重新估价
				// （旧实现固定半价预估，与降级后实际计费口径脱钩）
				if route.FallbackProvider == "" || route.FallbackModel == "" ||
					(route.FallbackProvider == route.Provider && route.FallbackModel == route.Model) {
					writeErr(w, common.ErrBudgetExhausted.Wrap(errors.New("no fallback model configured")))
					return
				}
				fbDims := pricing.Dims{TaskType: taskType, Model: route.FallbackModel, Provider: route.FallbackProvider, AgentID: agentID}
				fbSnapshot, ferr := h.pricing.ResolveSnapshot(ctx, ident.TenantID, customerID, fbDims)
				if ferr != nil {
					writeErr(w, common.ErrInternal.Wrap(ferr))
					return
				}
				reservation, err = h.ledgerSvc.Reserve(ctx, pool, h.estimateAmount(fbSnapshot, &chatReq), txnID, ttl)
				if err != nil {
					writeErr(w, common.FromErr(err))
					return
				}
				h.log.Warn("budget exhausted, fallback to cheaper model",
					"pool_id", pool.ID, "from", route.Model, "to", route.FallbackModel)
				route.Provider, route.Model = route.FallbackProvider, route.FallbackModel
				snapshot = fbSnapshot
			default: // block
				writeErr(w, common.ErrBudgetExhausted)
				return
			}
		}
	}

	// 7. Provider 适配器（fallback 可能已切换 provider）+ 渠道健康降级 + 密钥模型白名单
	// 7.1 健康检查：渠道被禁用时自动切换 fallback 渠道（均不可用则拒绝）
	if h.health != nil && h.health.IsDisabled(route.Provider) {
		if route.FallbackProvider != "" && route.FallbackProvider != route.Provider &&
			!h.health.IsDisabled(route.FallbackProvider) {
			h.log.Warn("provider disabled by health check, fallback",
				"from", route.Provider, "to", route.FallbackProvider)
			route.Provider, route.Model = route.FallbackProvider, route.FallbackModel
			// 切换模型后重取计价快照（不同模型费率不同，保证预留与计费同口径）
			hbDims := pricing.Dims{TaskType: taskType, Model: route.Model, Provider: route.Provider, AgentID: agentID}
			hbSnapshot, herr := h.pricing.ResolveSnapshot(ctx, ident.TenantID, customerID, hbDims)
			if herr != nil {
				if reservation != nil {
					_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
				}
				writeErr(w, common.ErrInternal.Wrap(herr))
				return
			}
			snapshot = hbSnapshot
		} else {
			if reservation != nil {
				_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
			}
			writeErr(w, common.ErrNoProvider.Wrap(fmt.Errorf("provider %q disabled (health)", route.Provider)))
			return
		}
	}
	// 7.2 密钥模型白名单：校验最终转发模型（路由/fallback 调整后），密钥约束优先于路由策略
	if !rbac.ModelAllowed(ident.ModelAllowList, route.Model) {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
		}
		writeErr(w, common.ErrModelNotAllowed.Wrap(fmt.Errorf("model %q not in key allow list", route.Model)))
		return
	}
	adapter, ok := h.providers.Get(route.Provider)
	if !ok {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
		}
		writeErr(w, common.ErrNoProvider.Wrap(fmt.Errorf("provider %q", route.Provider)))
		return
	}

	// 7.3 上游 Provider 渠道并发限制（保护上游账号不被打爆；fail-open）
	if h.limiter != nil {
		release, ok, err := h.limiter.Acquire(ctx, providerConcKey(route.Provider), h.cfg.RateLimit.ProviderMaxConcurr)
		if err != nil {
			h.log.Warn("provider concurrency check failed (fail-open)", "provider", route.Provider, "err", err)
		} else if !ok {
			if reservation != nil {
				_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
			}
			writeErr(w, common.ErrConcurrencyLimited.Wrap(fmt.Errorf("provider %q", route.Provider)))
			return
		} else if release != nil {
			defer release()
		}
	}

	// 9. 转发 LLM（计价快照已前移至 5.8，路由/降级切换处已同步重取）
	// 9.1 路由/降级切换模型后重写请求体 model 字段：上游按 body.model 实际执行，
	// 不重写会导致转发模型与定价/白名单校验模型不一致（按 B 模型费率给 A 模型计费），
	// 且 fallback 渠道不承载原模型时上游直接 404/400，降级路由形同虚设。
	if route.Model != chatReq.Model {
		if body, err = rewriteBodyModel(body, route.Model); err != nil {
			if reservation != nil {
				_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
			}
			writeErr(w, common.ErrBadRequest.Wrap(err))
			return
		}
	}
	w.Header().Set("X-Meteron-Transaction-ID", txnID.String())
	if reservation != nil {
		w.Header().Set("X-Meteron-Reservation-ID", reservation.ID.String())
	}

	req := &provider.ChatRequest{Model: route.Model, Body: body, Stream: chatReq.Stream}
	upStart := time.Now()
	result, err := adapter.Chat(ctx, req)
	if err != nil {
		h.observeHealth(route.Provider, false, time.Since(upStart))
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID) // 未产生 usage：全额释放
		}
		// 审计：网络层失败请求也入计量明细（status_code=502，tokens=0）
		h.emitFailureEvent(ctx, ident.TenantID, customerID, txnID, route, taskType, agentID, snapshot, body, reservation, idemKey, chatReq.Model, http.StatusBadGateway)
		writeErr(w, common.ErrUpstream.Wrap(err))
		return
	}
	h.observeHealth(route.Provider, result.StatusCode < 400, time.Since(upStart))
	if result.StatusCode >= 400 {
		if reservation != nil {
			_, _ = h.ledgerSvc.Release(ctx, ident.TenantID, reservation.ID)
		}
		// 审计：上游 4xx/5xx 请求入计量明细（真实 status_code，tokens=0）
		h.emitFailureEvent(ctx, ident.TenantID, customerID, txnID, route, taskType, agentID, snapshot, body, reservation, idemKey, chatReq.Model, result.StatusCode)
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
		ev := h.buildEvent(ident.TenantID, customerID, txnID, route, taskType, agentID, snapshot, usage, body, reservation, idemKey, chatReq.Model, result.StatusCode)
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
		if result.Usage == nil {
			// 上游 2xx 但缺 usage 字段（部分 vLLM/代理实现如此）：发 tokens=0
			// 审计事件（metadata 标记 missing_usage，可按模型/渠道排查上游契约异常）。
			// 不 confirm——无用量数据，预留等待过期释放（与流式中断同语义）。
			h.emitMissingUsageEvent(ctx, ident.TenantID, customerID, txnID, route, taskType, agentID,
				snapshot, body, reservation, idemKey, chatReq.Model, result.StatusCode)
		} else {
			finish(result.Usage)
		}
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
	Model     string          `json:"model"`
	Messages  []chatMessage   `json:"messages"`
	MaxTokens *int            `json:"max_tokens"`
	Stream    bool            `json:"stream"`
	Tools     json.RawMessage `json:"tools"` // 工具调用（auto 路由复杂度信号：tools 非空 → premium）
}

// maxTokensLimit max_tokens 合法上限：拒绝天文数字（如 MaxInt64 会让
// estimateTokens 的 int64 转换溢出为负值，从而绕过 TPM 限流并污染滑动窗口）。
// 主流 LLM 上游的实际上限远低于此值。
const maxTokensLimit = 1_000_000

// validateChatRequest 网关入口请求静态校验。
func validateChatRequest(req *chatRequest) error {
	if len(req.Messages) == 0 {
		return errors.New("messages required")
	}
	if req.MaxTokens != nil && (*req.MaxTokens < 0 || *req.MaxTokens > maxTokensLimit) {
		return fmt.Errorf("max_tokens must be in [0, %d]", maxTokensLimit)
	}
	return nil
}

// customerAllowed 客户级密钥绑定校验：零值 = 平台级密钥（不限制），
// 否则请求头指定的客户必须与密钥绑定客户一致（计费归属完整性）。
func customerAllowed(keyBinding, requested uuid.UUID) bool {
	return keyBinding == uuid.Nil || keyBinding == requested
}

// rewriteBodyModel 路由/降级切换模型后重写请求体 model 字段。
// 用 map[string]json.RawMessage 反序列化可保留其余字段原样（不丢未知字段、不改写格式）。
func rewriteBodyModel(body []byte, model string) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("rewrite body model: %w", err)
	}
	mb, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	m["model"] = json.RawMessage(mb)
	return json.Marshal(m)
}

// buildProfile 构建 auto 路由请求画像（复杂度评估信号）：
// 显式 X-Meteron-Quality 头 > 启发式（图片/工具调用/输入规模）。
func (h *Handler) buildProfile(r *http.Request, req *chatRequest) router.RequestProfile {
	p := router.RequestProfile{
		Quality: strings.ToLower(strings.TrimSpace(r.Header.Get(headerQuality))),
	}
	chars := 0
	for _, m := range req.Messages {
		chars += m.textLen()
		if contentHasImage(m.Content) {
			p.HasImages = true
		}
	}
	cpt := h.cfg.Business.EstimateCharsPerToken
	if cpt <= 0 {
		cpt = 4
	}
	p.InputTokens = uint64(chars/cpt + 1)
	// tools 非空判断：null / 空数组均视为无工具
	if t := string(req.Tools); t != "" && t != "null" && t != "[]" {
		p.HasTools = true
	}
	return p
}

// contentHasImage 检测多模态消息是否含图片（OpenAI content parts 的 type=image_url）。
func contentHasImage(content json.RawMessage) bool {
	var parts []struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(content, &parts); err != nil {
		return false
	}
	for _, p := range parts {
		if p.Type == "image_url" {
			return true
		}
	}
	return false
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

// estimateAmount 按计价快照单价估算本次请求的预留金额（与实际计费同口径）：
// 输入估算 token × input 单价 + 输出上限（max_tokens 或默认值）× output 单价，
// 再应用快照折扣。tiered 以首档单价近似（与 EventCost 单事件口径一致，
// 出账按累计阶梯精确重算）。零价快照（未命中费率规则）估算为 0——实际计费
// 亦为 0，口径一致；高价模型预留随真实费率水涨船高，封住超卖窗口。
// snap 为 nil 时回退全局配置单价（防御路径）。
func (h *Handler) estimateAmount(snap *pricing.Snapshot, req *chatRequest) decimal.Decimal {
	if snap == nil {
		return h.estimateAmountByConfig(req)
	}
	in, errIn := decimal.NewFromString(snap.InputUnitPrice)
	if errIn != nil {
		in = decimal.Zero
	}
	out, errOut := decimal.NewFromString(snap.OutputUnitPrice)
	if errOut != nil {
		out = decimal.Zero
	}
	est := in.Mul(decimal.NewFromInt(int64(h.estimateInputTokens(req)))).
		Add(out.Mul(decimal.NewFromInt(int64(h.estimateOutputCeiling(req)))))
	if d, derr := decimal.NewFromString(snap.Discount); derr == nil && !d.IsZero() {
		est = est.Mul(decimal.NewFromInt(1).Sub(d))
	}
	if est.IsNegative() { // 非法负单价配置：钳为 0，避免 Reserve 拒绝整个请求
		return decimal.Zero
	}
	return est
}

// estimateAmountByConfig 全局配置单价估算（仅 snap==nil 防御路径）。
func (h *Handler) estimateAmountByConfig(req *chatRequest) decimal.Decimal {
	price, err := decimal.NewFromString(h.cfg.Business.EstimateUnitPrice)
	if err != nil {
		price = decimal.Zero
	}
	return price.Mul(decimal.NewFromInt(int64(h.estimateTokens(req))))
}

// estimateInputTokens 输入 token 估算（消息字符折算）。
func (h *Handler) estimateInputTokens(req *chatRequest) uint64 {
	chars := 0
	for _, m := range req.Messages {
		chars += m.textLen()
	}
	cpt := h.cfg.Business.EstimateCharsPerToken
	if cpt <= 0 {
		cpt = 4
	}
	return uint64(chars/cpt + 1)
}

// estimateOutputCeiling 输出 token 上限估算（max_tokens 或默认值）。
func (h *Handler) estimateOutputCeiling(req *chatRequest) int {
	maxTok := h.cfg.Business.EstimateDefaultMaxTokens
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		maxTok = *req.MaxTokens
	}
	if maxTok < 0 {
		maxTok = 0
	}
	return maxTok
}

// estimateTokens 预估本次请求 token 数：输入字符折算 + max_tokens 上限。
// 供 TPM 限流与配置兜底估价共用。
func (h *Handler) estimateTokens(req *chatRequest) uint64 {
	return h.estimateInputTokens(req) + uint64(h.estimateOutputCeiling(req))
}

// clientIP 提取来源 IP（密钥 IP 白名单的判定依据）。
// 实现位于 rbac.ClientIP（与管理 API 共用同一可信代理语义），此处保留薄封装供本包调用。
func clientIP(r *http.Request, trustedProxies []string) string {
	return rbac.ClientIP(r, trustedProxies)
}

// 客户维度限流 key（Redis 键空间：rl = rate limit）
func customerRateKey(tenantID, customerID uuid.UUID, dim string) string {
	return "rl:c:" + tenantID.String() + ":" + customerID.String() + ":" + dim
}

func providerConcKey(providerName string) string {
	return "rl:p:" + providerName + ":conc"
}

// applyCustomerRateLimits 客户维度双重限流：
// RPM（每分钟请求数，cost=1）/ TPM（每分钟 token 数，cost=预估 token）/ 最大并发。
// 返回 (release, true) 放行（release 可为 nil=未启用并发限制），(nil, false) 已写错误响应。
// Redis 故障时 fail-open（限流是保护性措施，不应因缓存故障阻断业务）。
func (h *Handler) applyCustomerRateLimits(w http.ResponseWriter, ctx context.Context, tenantID, customerID uuid.UUID, chatReq *chatRequest) (func(), bool) {
	if h.limiter == nil {
		return nil, true
	}
	rl := h.cfg.RateLimit
	// RPM
	if rl.CustomerRPM > 0 {
		ok, err := h.limiter.AllowRate(ctx, customerRateKey(tenantID, customerID, "rpm"), rl.CustomerRPM, 1, time.Minute)
		if err != nil {
			h.log.Warn("customer rpm check failed (fail-open)", "customer_id", customerID, "err", err)
		} else if !ok {
			writeErr(w, common.ErrRateLimited.Wrap(fmt.Errorf("customer rpm limit %d/min", rl.CustomerRPM)))
			return nil, false
		}
	}
	// TPM
	if rl.CustomerTPM > 0 {
		ok, err := h.limiter.AllowRate(ctx, customerRateKey(tenantID, customerID, "tpm"), rl.CustomerTPM,
			int64(h.estimateTokens(chatReq)), time.Minute)
		if err != nil {
			h.log.Warn("customer tpm check failed (fail-open)", "customer_id", customerID, "err", err)
		} else if !ok {
			writeErr(w, common.ErrRateLimited.Wrap(fmt.Errorf("customer tpm limit %d/min", rl.CustomerTPM)))
			return nil, false
		}
	}
	// 并发
	if rl.CustomerMaxConcurr > 0 {
		release, ok, err := h.limiter.Acquire(ctx, customerRateKey(tenantID, customerID, "conc"), rl.CustomerMaxConcurr)
		if err != nil {
			h.log.Warn("customer concurrency check failed (fail-open)", "customer_id", customerID, "err", err)
			return nil, true
		}
		if !ok {
			writeErr(w, common.ErrConcurrencyLimited.Wrap(fmt.Errorf("customer max concurrent %d", rl.CustomerMaxConcurr)))
			return nil, false
		}
		return release, true
	}
	return nil, true
}

// observeHealth 真实流量结果计入渠道健康统计（nil 安全）。
func (h *Handler) observeHealth(providerName string, success bool, latency time.Duration) {
	if h.health != nil {
		h.health.Observe(providerName, success, latency)
	}
}

// emitFailureEvent 上游失败请求（网络错误 / 4xx / 5xx）审计事件：
// tokens=0、status_code 如实记录，账单不产生费用（预留已全额释放），
// 但审计日志可按状态码检索失败请求。
func (h *Handler) emitFailureEvent(
	ctx context.Context,
	tenantID, customerID, txnID uuid.UUID,
	route *router.RouteResult, taskType, agentID string,
	snapshot *pricing.Snapshot, rawBody []byte,
	reservation *ledger.Reservation, idemKey, requestedModel string,
	statusCode int,
) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	ev := h.buildEvent(tenantID, customerID, txnID, route, taskType, agentID, snapshot, nil, rawBody, reservation, idemKey, requestedModel, statusCode)
	if perr := h.producer.Produce(fctx, ev); perr != nil {
		h.log.Error("produce failure audit event failed", "event_id", ev.EventID, "err", perr)
	}
}

// emitMissingUsageEvent 上游 2xx 但响应缺 usage 字段的审计事件：
// tokens=0、status_code 如实记录、metadata 标记 missing_usage——
// 便于按 model/provider 检索上游契约异常（正常成功事件的 usage 不会缺失）。
// 不产生费用（预留不 confirm，等待 TTL 过期释放，与流式中断同语义）。
func (h *Handler) emitMissingUsageEvent(
	ctx context.Context,
	tenantID, customerID, txnID uuid.UUID,
	route *router.RouteResult, taskType, agentID string,
	snapshot *pricing.Snapshot, rawBody []byte,
	reservation *ledger.Reservation, idemKey, requestedModel string,
	statusCode int,
) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	ev := h.buildEvent(tenantID, customerID, txnID, route, taskType, agentID, snapshot, nil, rawBody, reservation, idemKey, requestedModel, statusCode)
	ev.Metadata["missing_usage"] = "true"
	if perr := h.producer.Produce(fctx, ev); perr != nil {
		h.log.Error("produce missing-usage audit event failed", "event_id", ev.EventID, "err", perr)
	}
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

// occupyIdempotencyKey 幂等键占用（独立于预算预留）：
// INSERT ... ON CONFLICT DO NOTHING 原子占用，冲突（RowsAffected=0）→ 重复提交。
// 占用与是否命中预算池无关——无池请求同样受保护，修复同 X-Idempotency-Key
// 重放产生双倍计量的问题。占用记录由 ScheduledWorker 按保留期清理
// （见 worker.CleanupIdempotencyKeys）。
func (h *Handler) occupyIdempotencyKey(ctx context.Context, tenantID, txnID uuid.UUID) error {
	tag, err := h.db.Exec(ctx, `
INSERT INTO idempotency_keys (tenant_id, transaction_id)
VALUES ($1::uuid, $2::uuid)
ON CONFLICT DO NOTHING`, tenantID.String(), txnID.String())
	if err != nil {
		return common.ErrInternal.Wrap(err)
	}
	if tag.RowsAffected() == 0 {
		return common.ErrDuplicateTransaction
	}
	return nil
}

func (h *Handler) buildEvent(
	tenantID, customerID, txnID uuid.UUID,
	route *router.RouteResult, taskType, agentID string,
	snapshot *pricing.Snapshot, usage *provider.Usage,
	rawBody []byte, reservation *ledger.Reservation, idemKey, requestedModel string,
	statusCode int,
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
	ev := &ingest.MeterEvent{
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
		PricingContextSnapshot: snapshot,
		Currency:               currency,
		Metadata:               metadata,
		RawPayload:             raw,
		StatusCode:             uint16(statusCode),
	}
	// usage=nil：上游失败审计事件（tokens 全零，仅审计检索，不产生费用）
	if usage != nil {
		ev.InputTokens = usage.InputTokens
		ev.OutputTokens = usage.OutputTokens
		ev.CachedInputTokens = usage.CachedInputTokens
	}
	return ev
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

// writeErr 统一错误响应。
//
// 5xx 属内部故障：错误详情可能包含 SQL、约束名、内部地址等，只记日志不回客户端
// （依赖 main 中已设置的 slog.Default）。4xx 为调用方可纠正的错误，
// 保留 detail 便于自助排障。
func writeErr(w http.ResponseWriter, err error) {
	biz := common.FromErr(err)
	detail := err.Error()
	if biz.HTTPStatus >= http.StatusInternalServerError {
		slog.Default().Error("gateway internal error", "code", biz.Code, "msg", biz.Msg, "err", err)
		detail = "internal error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(biz.HTTPStatus)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code": biz.Code,
		"msg":  biz.Msg,
		"data": map[string]string{"detail": detail},
	})
}
