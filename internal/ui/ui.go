// Package ui 内嵌 Web 控制台（Vue 3 + Vite 构建产物）。
// 挂载在 "/"，由 Go 1.22+ ServeMux 的精确匹配保证 /api、/v1、/metrics 优先。
//
// 构建产物来源：web/ 目录执行 `npm run build` 输出到本包 dist/；
// dist/ 内置占位页面，无 Node 环境时 go build 仍可通过（仅提示需构建前端）。
package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed dist
var distFS embed.FS

// Handler 返回控制台 HTTP handler（SPA 回退到 index.html）。
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic(err) // embed 静态目录保证存在
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(sub, p); err != nil {
			// SPA 回退：非资源路径（无扩展名）返回 index.html；
			// 带扩展名的资源直接走 FileServer 返回 404，避免 JS/CSS 缺失误返回 HTML
			if !strings.Contains(p, ".") {
				r.URL.Path = "/"
			}
		}
		fileServer.ServeHTTP(w, r)
	})
}
