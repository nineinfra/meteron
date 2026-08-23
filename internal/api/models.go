package api

import (
	"net/http"

	"github.com/meteron/meteron/internal/rbac"
)

// listModels 列出可用模型（providers 配置即事实来源）。
// 聊天窗口与客户端探测用；ScopeGateway（可发起对话的密钥）即可读。
func (s *Server) listModels(w http.ResponseWriter, r *http.Request) {
	if requireScope(w, r, rbac.ScopeGateway) == nil {
		return
	}
	type modelView struct {
		Model    string `json:"model"`
		Provider string `json:"provider"`
		Type     string `json:"type"`
	}
	out := []modelView{}
	for _, p := range s.cfg.Providers {
		for _, name := range p.Models {
			out = append(out, modelView{Model: name, Provider: p.Name, Type: p.Type})
		}
	}
	writeOK(w, out)
}
