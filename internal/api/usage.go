package api

import (
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- usage ----

// parseRange 解析时间区间参数，默认最近24小时。
func parseRange(r *http.Request) (from, to time.Time, err error) {
	now := time.Now().UTC()
	from, to = now.Add(-24*time.Hour), now
	if v := r.URL.Query().Get("start_time"); v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return from, to, errBadRequest("invalid start_time")
		}
		from = t.UTC()
	}
	if v := r.URL.Query().Get("end_time"); v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return from, to, errBadRequest("invalid end_time")
		}
		to = t.UTC()
	}
	if !to.After(from) {
		return from, to, errBadRequest("end_time must be after start_time")
	}
	return from, to, nil
}

func (s *Server) usage(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	customerID, err := customerFilter(r, ident)
	if err != nil {
		writeErr(w, err)
		return
	}
	from, to, err := parseRange(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	summaries, err := meter.QueryUsage(r.Context(), s.ch, ident.TenantID, customerID, from, to)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	if summaries == nil {
		summaries = []meter.UsageSummary{}
	}
	writeOK(w, map[string]interface{}{
		"from":       from,
		"to":         to,
		"customer_id": customerID,
		"items":      summaries,
	})
}

func (s *Server) usageTimeseries(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	customerID, err := customerFilter(r, ident)
	if err != nil {
		writeErr(w, err)
		return
	}
	from, to, err := parseRange(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	points, err := meter.QueryTimeseries(r.Context(), s.ch, ident.TenantID, customerID, from, to)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	if points == nil {
		points = []meter.TimeseriesPoint{}
	}
	writeOK(w, map[string]interface{}{
		"from":        from,
		"to":          to,
		"customer_id": customerID,
		"points":      points,
	})
}

// ---- ingest ----

// ingestEvent 外部计量事件上报（晚到事件回溯窗口校验 + Kafka 投递）。
func (s *Server) ingestEvent(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeIngest)
	if ident == nil {
		return
	}
	var req struct {
		EventID           string            `json:"event_id"`
		TransactionID     string            `json:"transaction_id"`
		CustomerID        string            `json:"customer_id"`
		EventType         string            `json:"event_type"`
		Timestamp         string            `json:"timestamp"`
		Provider          string            `json:"provider"`
		Model             string            `json:"model"`
		TaskType          string            `json:"task_type"`
		AgentID           string            `json:"agent_id"`
		InputTokens       uint64            `json:"input_tokens"`
		OutputTokens      uint64            `json:"output_tokens"`
		CachedInputTokens uint64            `json:"cached_input_tokens"`
		Currency          string            `json:"currency"`
		Metadata          map[string]string `json:"metadata"`
		RawPayload        string            `json:"raw_payload"`
	}
	if !decodeBody(w, r, &req) {
		return
	}

	eventID, err := uuid.Parse(req.EventID)
	if err != nil {
		writeErr(w, errBadRequest("invalid event_id"))
		return
	}
	txnID, err := uuid.Parse(req.TransactionID)
	if err != nil {
		writeErr(w, errBadRequest("invalid transaction_id"))
		return
	}
	if req.CustomerID == "" {
		writeErr(w, errBadRequest("customer_id required"))
		return
	}
	if ident.CustomerID != uuid.Nil && req.CustomerID != ident.CustomerID.String() {
		writeErr(w, common.ErrUnauthorized.Wrap(errBadRequest("customer_id mismatch with key scope")))
		return
	}
	customerID, err := uuid.Parse(req.CustomerID)
	if err != nil {
		writeErr(w, errBadRequest("invalid customer_id"))
		return
	}

	ts := time.Now().UTC()
	if req.Timestamp != "" {
		t, perr := time.Parse(time.RFC3339, req.Timestamp)
		if perr != nil {
			writeErr(w, errBadRequest("invalid timestamp"))
			return
		}
		ts = t.UTC()
	}

	// 晚到事件回溯窗口校验（§3）：窗口内合法晚到；未来事件轻微偏移容忍1分钟
	lookback := time.Duration(s.cfg.Business.EventLookbackWindowDays) * 24 * time.Hour
	if ts.Before(time.Now().UTC().Add(-lookback)) {
		writeErr(w, common.ErrEventTimeOutOfWindow)
		return
	}
	if ts.After(time.Now().UTC().Add(time.Minute)) {
		writeErr(w, common.ErrEventTimeOutOfWindow.Wrap(errBadRequest("timestamp in future")))
		return
	}

	if req.EventType == "" {
		req.EventType = "llm_usage"
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}

	ev := &ingest.MeterEvent{
		EventID:           eventID,
		TransactionID:     txnID,
		EventVersion:      1,
		TenantID:          ident.TenantID,
		CustomerID:        customerID,
		EventType:         req.EventType,
		Timestamp:         ts,
		Provider:          req.Provider,
		Model:             req.Model,
		TaskType:          req.TaskType,
		AgentID:           req.AgentID,
		InputTokens:       req.InputTokens,
		OutputTokens:      req.OutputTokens,
		CachedInputTokens: req.CachedInputTokens,
		Currency:          req.Currency,
		Metadata:          req.Metadata,
		RawPayload:        req.RawPayload,
	}
	// 外部上报事件无快照：按零价快照兜底（营收侧配置兜底规则保障）
	if err := s.producer.Produce(r.Context(), ev); err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]interface{}{
		"event_id": ev.EventID,
		"accepted": true,
	})
}
