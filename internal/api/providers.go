package api

import (
	"errors"
	"net/http"

	"github.com/meteron/meteron/internal/common"
)

// ---- Provider 渠道健康管理（能力2）----

// providerHealth GET /api/v1/providers/health
// 各渠道错误率、平均延迟、禁用状态（含真实流量与主动探测样本）。
func (s *Server) providerHealth(w http.ResponseWriter, r *http.Request) {
	if requireScope(w, r, "admin") == nil {
		return
	}
	if s.health == nil {
		writeErr(w, common.ErrNotImplemented.Wrap(errors.New("health tracker not enabled")))
		return
	}
	writeOK(w, s.health.Snapshot())
}

// enableProvider POST /api/v1/providers/{name}/enable
// 人工恢复渠道（清除自动禁用并重置统计，从零重新观察）。
func (s *Server) enableProvider(w http.ResponseWriter, r *http.Request) {
	if requireScope(w, r, "admin") == nil {
		return
	}
	if s.health == nil {
		writeErr(w, common.ErrNotImplemented.Wrap(errors.New("health tracker not enabled")))
		return
	}
	name := r.PathValue("name")
	s.health.Enable(name)
	writeOK(w, s.health.SnapshotOf(name))
}

// disableProvider POST /api/v1/providers/{name}/disable
// 人工禁用渠道（路由引擎即刻跳过）。
func (s *Server) disableProvider(w http.ResponseWriter, r *http.Request) {
	if requireScope(w, r, "admin") == nil {
		return
	}
	if s.health == nil {
		writeErr(w, common.ErrNotImplemented.Wrap(errors.New("health tracker not enabled")))
		return
	}
	name := r.PathValue("name")
	s.health.Disable(name)
	writeOK(w, s.health.SnapshotOf(name))
}
