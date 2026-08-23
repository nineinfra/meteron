# Meteron 多阶段构建（前端 Vue → Go 静态二进制 → 运行时镜像）

# ---- 阶段 1：前端构建（Vue 3 + Vite）----
FROM node:22.22.2-bookworm AS frontend

WORKDIR /web
ENV npm_config_registry=https://registry.npmmirror.com

# 先拷贝依赖清单，利用层缓存
COPY web/package.json web/package-lock.json* ./
RUN npm install --no-audit --no-fund

COPY web/ ./
# vite outDir=../internal/ui/dist（相对 /web），产物实际落在 /internal/ui/dist
RUN npm run build

# ---- 阶段 2：Go 构建（embed 前端产物）----
FROM golang:1.24-alpine AS builder

WORKDIR /src
ENV GOPROXY=https://goproxy.cn,direct \
    CGO_ENABLED=0 \
    GOOS=linux

# 先拷贝依赖清单，利用层缓存
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# 覆盖占位 dist 为真实前端构建产物
COPY --from=frontend /internal/ui/dist ./internal/ui/dist

RUN go build -trimpath -ldflags="-s -w" -o /out/meteron ./cmd/meteron

# ---- 阶段 3：运行时镜像 ----
FROM alpine:3.20

RUN adduser -D -u 10001 meteron
USER meteron

COPY --from=builder /out/meteron /usr/local/bin/meteron

EXPOSE 8080
ENTRYPOINT ["meteron"]
