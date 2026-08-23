#!/usr/bin/env bash
# Meteron 端到端 Demo（仅依赖 bash + curl，无需 jq）
#
# 前置：docker compose up -d --build
#
# 流程：供给客户/产品/费率/合同/预算池 → Gateway 对话（mock provider）
#       → 预算预留与确认 → Kafka→CH 计量 → 用量查询 → BillingRun → 账单定稿
set -euo pipefail

BASE="${METERON_BASE:-http://localhost:8080}"
KEY="${METERON_KEY:-mk_meteron_bootstrap_dev_key}"
AUTH="Authorization: Bearer ${KEY}"

req() { # method path [json-body]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -X "$method" "${BASE}${path}" -H "$AUTH" -H "Content-Type: application/json" -d "$body"
  else
    curl -sS -X "$method" "${BASE}${path}" -H "$AUTH"
  fi
}

# 从 JSON 响应提取第一个 "id":"<uuid>" 字段（纯 sed 实现，无 jq 依赖）
field_id() {
  printf '%s' "$1" | tr ',{' '\n\n' | sed -n '/"id"/{s/.*"id"[[:space:]]*:[[:space:]]*"\([0-9a-f-]\{36\}\)".*/\1/p; q}'
}

echo "== 0. 健康检查 =="
until curl -sSf "${BASE}/healthz" >/dev/null 2>&1; do sleep 2; done
echo "服务就绪"

echo "== 1. 供给：客户 / 计量指标 / 产品 / 费率卡 / 规则 / 合同 / 预算池 =="
CUSTOMER=$(field_id "$(req POST /api/v1/customers '{"name":"Acme Corp","external_id":"acme","currency":"USD"}')")
METRIC=$(field_id "$(req POST /api/v1/billable-metrics '{"name":"llm_tokens","aggregation_type":"sum"}')")
PRODUCT=$(field_id "$(req POST /api/v1/products "{\"name\":\"llm-chat\",\"display_name\":\"LLM Chat\",\"metric_id\":\"${METRIC}\"}")")
RATECARD=$(field_id "$(req POST /api/v1/rate-cards '{"name":"standard-2026","currency":"USD"}')")
RULE=$(field_id "$(req POST /api/v1/rate-cards/${RATECARD}/rules "{\"product_id\":\"${PRODUCT}\",\"match_conditions\":{\"task_type\":\"*\"},\"pricing_type\":\"flat\",\"pricing_config\":{\"input_unit_price\":\"0.000003\",\"output_unit_price\":\"0.000006\",\"cached_input_unit_price\":\"0.0000003\"},\"is_default_rule\":true}")")
CONTRACT=$(field_id "$(req POST /api/v1/contracts "{\"customer_id\":\"${CUSTOMER}\",\"rate_card_id\":\"${RATECARD}\"}")")
POOL=$(field_id "$(req POST /api/v1/budget-pools "{\"customer_id\":\"${CUSTOMER}\",\"max_budget\":\"10\",\"warning_threshold\":\"2\",\"exhaust_policy\":\"warn\"}")")
echo "customer=${CUSTOMER}"
echo "pool=${POOL}"

echo "== 2. Gateway 对话（mock provider，非流式）=="
curl -sS -X POST "${BASE}/v1/chat/completions" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -H "X-Meteron-Customer-ID: ${CUSTOMER}" -H "X-Meteron-Task-Type: chat" \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"你好，Meteron！"}]}'
echo

echo "== 2b. Gateway 对话（流式 SSE）=="
curl -sS -N -X POST "${BASE}/v1/chat/completions" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -H "X-Meteron-Customer-ID: ${CUSTOMER}" -H "X-Meteron-Task-Type: chat" \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"流式测试"}],"stream":true}' \
  | grep -c '^data:' | xargs -I{} echo "（SSE 完成，共 {} 个 chunk）"

echo "== 3. 等待异步链路（Kafka→Sink→ClickHouse）=="
sleep 5

echo "== 4. 用量查询 =="
req GET "/api/v1/usage?customer_id=${CUSTOMER}"
echo

echo "== 5. 预算池余额（PG 可信源 + Redis 缓存）=="
req GET "/api/v1/budget-pools/${POOL}/balance"
echo

echo "== 6. 手动触发 BillingRun（今日周期）=="
TODAY=$(date -u +%Y-%m-%dT00:00:00Z)
TOMORROW=$(date -u -d "+1 day" +%Y-%m-%dT00:00:00Z 2>/dev/null || date -u -v+1d +%Y-%m-%dT00:00:00Z)
req POST /api/v1/billing-runs "{\"period_start\":\"${TODAY}\",\"period_end\":\"${TOMORROW}\"}"
echo

echo "== 7. 账单查询与定稿 =="
BILLS=$(req GET "/api/v1/bills?customer_id=${CUSTOMER}")
echo "${BILLS}"
BILL=$(field_id "${BILLS}")
req POST "/api/v1/bills/${BILL}/finalize"
echo

echo "== 8. 对账任务列表（定时 Worker 自动运行）=="
req GET /api/v1/reconciliation-runs
echo

echo "== Demo 完成 =="
