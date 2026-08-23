import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 构建产物直接输出到 Go embed 目录（internal/ui/dist）；
// base 用相对路径，任意挂载前缀均可服务。
// 开发模式：npm run dev 后 /api 代理到本地 Meteron（:8080）。
export default defineConfig({
  plugins: [vue()],
  base: './',
  build: {
    outDir: '../internal/ui/dist',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8080',
      '/v1': 'http://localhost:8080',
    },
  },
})
