#!/usr/bin/env bash
# Meteron 端到端验证脚本（仅依赖 bash + curl，无需 jq）
#
# 幂等：资源已存在则复用，可重复执行；用于部署后的全链路健康验证。
#
# 前置：docker compose -f deploy/docker-compose.yml up -d --build
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

# must_id：提取 id 失败时打印响应并中止（fail-fast 诊断）
must_id() {
  local label="$1" resp="$2" id
  id=$(field_id "$resp")
  if [ -z "$id" ]; then
    echo "FATAL: ${label} 创建失败，响应：" >&2
    printf '%s\n' "$resp" >&2
    exit 1
  fi
  printf '%s' "$id"
}

# 从列表 JSON 中按 external_id / name 提取对应条目的 id（幂等重跑用）
find_by_ext() { # json_list external_id
  printf '%s' "$1" | tr '{' '\n' | sed -n "/\"external_id\"[[:space:]]*:[[:space:]]*\"$2\"/{s/.*\"id\"[[:space:]]*:[[:space:]]*\"\([0-9a-f-]\{36\}\)\".*/\1/p; q}"
}
find_by_name() { # json_list name
  printf '%s' "$1" | tr '{' '\n' | sed -n "/\"name\"[[:space:]]*:[[:space:]]*\"$2\"/{s/.*\"id\"[[:space:]]*:[[:space:]]*\"\([0-9a-f-]\{36\}\)\".*/\1/p; q}"
}
# 通用：按任意字符串字段（如 product_id / customer_id）提取对应条目的 id
find_by_field() { # json_list field value
  printf '%s' "$1" | tr '{' '\n' | sed -n "/\"$2\"[[:space:]]*:[[:space:]]*\"$3\"/{s/.*\"id\"[[:space:]]*:[[:space:]]*\"\([0-9a-f-]\{36\}\)\".*/\1/p; q}"
}

echo "== 0. 健康检查 =="
until curl -sSf "${BASE}/healthz" >/dev/null 2>&1; do sleep 2; done
echo "服务就绪"

echo "== 1. 供给：客户 / 计量指标 / 产品 / 费率卡 / 规则 / 合同 / 预算池（幂等，可重复运行）=="
# 客户：已存在（external_id=acme）则复用
EXISTING=$(req GET /api/v1/customers)
CUSTOMER=$(find_by_ext "$EXISTING" "acme")
if [ -z "$CUSTOMER" ]; then
  CUSTOMER=$(must_id "customer" "$(req POST /api/v1/customers '{"name":"Acme Corp","external_id":"acme","currency":"USD"}')")
  echo "客户已创建"
else
  echo "客户已存在，复用"
fi
METRIC=$(find_by_name "$(req GET /api/v1/billable-metrics)" "llm_tokens")
if [ -z "$METRIC" ]; then
  METRIC=$(must_id "billable-metric" "$(req POST /api/v1/billable-metrics '{"name":"llm_tokens","aggregation_type":"sum"}')")
fi
PRODUCT=$(find_by_name "$(req GET /api/v1/products)" "llm-chat")
if [ -z "$PRODUCT" ]; then
  PRODUCT=$(must_id "product" "$(req POST /api/v1/products "{\"name\":\"llm-chat\",\"display_name\":\"LLM Chat\",\"metric_id\":\"${METRIC}\"}")")
fi
RATECARD=$(find_by_name "$(req GET /api/v1/rate-cards)" "standard-2026")
if [ -z "$RATECARD" ]; then
  RATECARD=$(must_id "rate-card" "$(req POST /api/v1/rate-cards '{"name":"standard-2026","currency":"USD"}')")
fi
# 规则：该费率卡下已存在同产品规则则复用
RULE=$(find_by_field "$(req GET /api/v1/rate-cards/${RATECARD}/rules)" "product_id" "${PRODUCT}")
if [ -z "$RULE" ]; then
  RULE=$(must_id "rate-rule" "$(req POST /api/v1/rate-cards/${RATECARD}/rules "{\"product_id\":\"${PRODUCT}\",\"match_conditions\":{\"task_type\":\"*\"},\"pricing_type\":\"flat\",\"pricing_config\":{\"input_unit_price\":\"0.000003\",\"output_unit_price\":\"0.000006\",\"cached_input_unit_price\":\"0.0000003\"},\"is_default_rule\":true}")")
  echo "费率规则已创建"
else
  echo "费率规则已存在，复用"
fi
# 合同：该客户已有生效合同则复用
CONTRACT=$(find_by_field "$(req GET "/api/v1/contracts?customer_id=${CUSTOMER}")" "customer_id" "${CUSTOMER}")
if [ -z "$CONTRACT" ]; then
  CONTRACT=$(must_id "contract" "$(req POST /api/v1/contracts "{\"customer_id\":\"${CUSTOMER}\",\"rate_card_id\":\"${RATECARD}\"}")")
  echo "合同已创建"
else
  echo "合同已存在，复用"
fi
# 预算池：该客户已有池子则复用
POOL=$(find_by_field "$(req GET "/api/v1/budget-pools?customer_id=${CUSTOMER}")" "customer_id" "${CUSTOMER}")
if [ -z "$POOL" ]; then
  POOL=$(must_id "budget-pool" "$(req POST /api/v1/budget-pools "{\"customer_id\":\"${CUSTOMER}\",\"max_budget\":\"10\",\"warning_threshold\":\"2\",\"exhaust_policy\":\"warn\"}")")
  echo "预算池已创建"
else
  echo "预算池已存在，复用"
fi
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

echo "== 验证完成 =="
