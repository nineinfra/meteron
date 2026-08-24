#!/usr/bin/env bash
set -euo pipefail

# Meteron K8s 部署辅助脚本
# 用法:
#   ./deploy/k8s/apply.sh dev     # 部署 dev 配置（小规格）
#   ./deploy/k8s/apply.sh prod    # 部署 prod 配置（生产高配）
#   ./deploy/k8s/apply.sh base    # 部署 base 默认配置
#   ./deploy/k8s/apply.sh dry dev # 预览 dev 渲染结果（不实际部署）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV="${1:-dev}"
DRY="${2:-}"

case "$ENV" in
  dev|prod)
    KUSTOM_DIR="$SCRIPT_DIR/overlays/$ENV"
    ;;
  base)
    KUSTOM_DIR="$SCRIPT_DIR/base"
    ;;
  *)
    echo "用法: $0 {dev|prod|base} [dry]"
    echo "  dev  — 开发环境（小规格资源）"
    echo "  prod — 生产环境（高配资源）"
    echo "  base — 默认中等配置"
    exit 1
    ;;
esac

if [ "$DRY" = "dry" ]; then
  echo "=== 预览 $ENV 配置（不实际部署）==="
  kubectl kustomize "$KUSTOM_DIR"
else
  echo "=== 部署 $ENV 配置 ==="
  kubectl apply -k "$KUSTOM_DIR"
  echo ""
  echo "等待 Pod 就绪..."
  kubectl -n meteron wait --for=condition=Ready pod --all --timeout=300s 2>/dev/null || true
  echo ""
  kubectl -n meteron get pods -o wide
fi
