# Meteron 多阶段构建（静态二进制，无 CGO 依赖）
FROM golang:1.24-alpine AS builder

WORKDIR /src
ENV GOPROXY=https://goproxy.cn,direct \
    CGO_ENABLED=0 \
    GOOS=linux

# 先拷贝依赖清单，利用层缓存
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/meteron ./cmd/meteron

# 运行时镜像
FROM alpine:3.20

RUN adduser -D -u 10001 meteron
USER meteron

COPY --from=builder /out/meteron /usr/local/bin/meteron

EXPOSE 8080
ENTRYPOINT ["meteron"]
