package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/meteron/meteron/internal/common"
	"github.com/meteron/meteron/internal/rbac"
)

// ---- API Key 管理（能力1：IP/模型白名单、过期、绑定客户、启停）----

// createAPIKey POST /api/v1/api-keys
// body: {customer_id?, scopes?, expire_at?, ip_whitelist?, model_allow_list?}
// 明文密钥仅此一次返回。
func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	var req struct {
		CustomerID     string   `json:"customer_id"`
		Scopes         []string `json:"scopes"`
		ExpireAt       string   `json:"expire_at"` // RFC3339，空=永不过期
		IPWhitelist    []string `json:"ip_whitelist"`
		ModelAllowList []string `json:"model_allow_list"`
	}
	if !decodeBody(w, r, &req) {
		return
	}

	opts := rbac.KeyOptions{
		Scopes:         req.Scopes,
		IPWhitelist:    req.IPWhitelist,
		ModelAllowList: req.ModelAllowList,
	}
	if req.CustomerID != "" {
		cid, err := uuid.Parse(req.CustomerID)
		if err != nil {
			writeErr(w, errBadRequest("invalid customer_id"))
			return
		}
		// 校验客户存在且属于当前租户（防止跨租户绑定）
		var exists bool
		if err := s.db.QueryRow(r.Context(),
			`SELECT EXISTS(SELECT 1 FROM customers WHERE id=$1::uuid AND tenant_id=$2::uuid AND deleted_at IS NULL)`,
			cid.String(), ident.TenantID.String()).Scan(&exists); err != nil {
			writeErr(w, common.ErrInternal.Wrap(err))
			return
		}
		if !exists {
			writeErr(w, common.ErrNotFound.Wrap(errors.New("customer")))
			return
		}
		opts.CustomerID = &cid
	}
	if req.ExpireAt != "" {
		t, err := time.Parse(time.RFC3339, req.ExpireAt)
		if err != nil {
			writeErr(w, errBadRequest("invalid expire_at (RFC3339 required)"))
			return
		}
		// DB 列为无时区 TIMESTAMP：pgx 按 time 自身时区的墙钟编码、丢弃偏移，
		// 而 NOW() 按 DB 会话时区（UTC）求值——带非零偏移的输入统一转 UTC，
		// 否则实际失效点可比预期晚数小时（如 +08:00 输入晚 8 小时）
		t = t.UTC()
		opts.ExpireAt = &t
	}

	id, plaintext, err := s.auth.CreateKey(r.Context(), ident.TenantID, opts)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]interface{}{
		"id":       id,
		"api_key":  plaintext, // 仅此一次返回
		"scopes":   opts.Scopes,
		"note":     "请妥善保管，明文不再二次展示",
	})
}

// listAPIKeys GET /api/v1/api-keys（不含哈希与明文）
func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	keys, err := s.auth.ListKeys(r.Context(), ident.TenantID)
	if err != nil {
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, keys)
}

// setAPIKeyEnabled PATCH /api/v1/api-keys/{id}/enabled  body: {enabled: bool}
func (s *Server) setAPIKeyEnabled(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if err := s.auth.SetKeyEnabled(r.Context(), ident.TenantID, id, req.Enabled); err != nil {
		if errors.Is(err, rbac.ErrKeyNotFound) {
			writeErr(w, common.ErrNotFound.Wrap(errors.New("api key")))
			return
		}
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]bool{"enabled": req.Enabled})
}

// deleteAPIKey DELETE /api/v1/api-keys/{id}（软删除）
func (s *Server) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	ident := requireScope(w, r, rbac.ScopeAdmin)
	if ident == nil {
		return
	}
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	if err := s.auth.DeleteKey(r.Context(), ident.TenantID, id); err != nil {
		if errors.Is(err, rbac.ErrKeyNotFound) {
			writeErr(w, common.ErrNotFound.Wrap(errors.New("api key")))
			return
		}
		writeErr(w, common.ErrInternal.Wrap(err))
		return
	}
	writeOK(w, map[string]string{"status": "deleted"})
}
