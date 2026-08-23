# Meteron

自托管的 **LLM 计量计费网关**：应用把 LLM 请求指向 Meteron，即可获得 token 级计量、实时预算管控、精确计费与自动对账。数据 100% 自持，不经过任何第三方。

## 核心价值

| 能力 | 说明 |
|------|------|
| **一行接入** | OpenAI 兼容协议透明转发，应用只改 `base_url` 和鉴权头，无 SDK 依赖 |
| **实时预算管控** | 请求前预留、响应后确认的两阶段预算；超支可阻断/告警/自动降级廉价模型，绝不超卖 |
| **精确计费** | 计量事件内嵌费率快照，费率改价不影响历史账单；支持阶梯计价与缓存 token 差异化定价 |
| **账实一致** | 计量明细（ClickHouse）与账本流水（PostgreSQL）双轨自动对账，差异自动生成红冲/补差流水 |
| **Web 控制台** | 内嵌可视化看板：token 用量趋势、模型用量明细、预算池余额、账单管理，零前端依赖 |
| **数据自持** | 全栈自托管（PG / ClickHouse / Redis / Kafka），多租户 + RBAC 隔离 |

## 快速开始（2 条命令）

```bash
docker compose up -d --build   # 启动全栈 + Meteron（内置 mock provider，无需任何 API Key）
./scripts/verify.sh            # 端到端验证：建客户→对话→计量→出账→定稿→对账
```

> 验证脚本仅依赖 `bash` + `curl`，幂等可重复执行。服务就绪约需 30~60 秒（首次拉镜像较慢）。

## Web 控制台

启动后浏览器打开 **`http://localhost:8080/`**，输入 API Key（开发环境默认
`mk_meteron_bootstrap_dev_key`）即可看到：

- **KPI 卡片**：请求数、输入/输出/缓存命中 tokens（支持 24h / 7d / 30d 切换）
- **用量趋势图**：小时粒度输入/输出 token 堆叠柱状图（超 96 点自动按天聚合）
- **用量明细**：按模型 × Provider × 任务类型的 token 消耗与请求数
- **预算池**：实时余额、花费进度条（80% 预警 / 100% 告警变色）、在途预留、超支策略
- **账单**：金额、账期、状态，draft 账单一键定稿

控制台为内嵌静态资源（同源调用 Management API，无外部 CDN 依赖），每 60 秒自动刷新。

## 30 秒手工体验

自动验证之外，核心闭环手工只需三步（`KEY` 为引导 API Key）：

```bash
# 1. 建客户（拿到 customer_id）
curl -sX POST localhost:8080/api/v1/customers \
  -H "Authorization: Bearer mk_meteron_bootstrap_dev_key" \
  -H "Content-Type: application/json" \
  -d '{"name":"Acme","external_id":"acme","currency":"USD"}'

# 2. 发起对话（OpenAI 兼容，X-Meteron-Customer-ID 支持 UUID 或 external_id）
curl -sX POST localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer mk_meteron_bootstrap_dev_key" \
  -H "X-Meteron-Customer-ID: acme" \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"hello"}]}'

# 3. 查用量（异步链路 Kafka→ClickHouse，约 5 秒后可查；customer_id 需用第 1 步返回的 UUID）
curl -s "localhost:8080/api/v1/usage?customer_id=<第1步返回的id>" \
  -H "Authorization: Bearer mk_meteron_bootstrap_dev_key"
```

计费、预算、对账是同一套数据的后台加工：`/api/v1/budget-pools`（预算池）、`/api/v1/billing-runs`（出账跑批）、`/api/v1/reconciliation-runs`（对账结果）。

## 工作原理

```
应用 ──OpenAI 兼容──▶ Gateway ──转发──▶ LLM Provider (OpenAI/Anthropic/GLM/本地模型/mock)
                       │ 预算预留(Redis+PG)      │ usage 嗅探(含 SSE 流式)
                       ▼                         ▼
                    Kafka meter-events ──▶ ClickHouse 明细 + 小时预聚合
                                                │
                     BillingRun(按快照计价) ──▶ PG 账单 + 不可变账本流水
                                                │
                     对账 Worker ◀── 计量 vs 账本自动校准（差异→correction 流水）
```

- **在线链路**只做轻量预算校验与转发（毫秒级）；计价、记账、出账全部异步，不阻塞请求。
- **幂等保证**：事件按 `event_id` 去重，账单跑批可安全重跑，同一 `Idempotency-Key` 不重复计费。

## 生产级使用步骤

按顺序执行即可从验证环境升级为生产环境。

### 1. 密钥加固（必做）

```bash
# 生成 32 字节 AES key（用于加密存储上游 LLM API Key）
openssl rand -hex 32
```

在部署环境注入并重启（容器部署全部走环境变量，见 docker-compose.yml）：

```bash
METERON_AES_KEY=<新key>                  # 加密上游 API Key，泄漏=上游账号被盗用
METERON_BOOTSTRAP_API_KEY=<强随机值>     # 首次启动自动创建为 admin 权限密钥
```

> 引导 Key 即当前的管理入口（admin 全权限）。轮换方式：改环境变量重启会在原租户下
> 自动新增一条 key 记录，旧 key 通过数据库禁用（`UPDATE api_keys SET deleted_at=NOW() WHERE ...`）。
> 最小权限 API Key（按客户/按 scope 授权）为 V1 路线项，MVP 阶段统一走引导 Key。

### 2. 接入真实 Provider

`config.yaml` 配置后重启（`api_key` 会以 AES 加密落库）：

```yaml
providers:
  - name: openai-main
    type: openai            # openai | openai-compatible | local | anthropic | mock
    base_url: "https://api.openai.com/v1"
    api_key: "sk-..."
    models: ["gpt-4o", "gpt-4o-mini"]
  - name: zhipu
    type: openai            # GLM 兼容模式同样走 openai 类型
    base_url: "https://open.bigmodel.cn/api/paas/v4"
    api_key: "..."
    models: ["glm-4.6"]
  - name: ollama            # 本地部署模型：Ollama / vLLM / LM Studio 等 OpenAI 兼容端点，api_key 可留空
    type: local
    base_url: "http://localhost:11434/v1"
    models: ["llama3.1", "qwen2.5:7b"]
```

### 3. 完成计费供给（每个计费客户一次）

7 个 API 依次调用，完整参数见 `scripts/verify.sh` 第 1 段（可直接复制改值）：

```
客户 → 计量指标(billable-metrics) → 产品(products) → 费率卡(rate-cards)
     → 计价规则(rate-cards/{id}/rules) → 合同(contracts) → 预算池(budget-pools)
```

- **费率规则**决定计价：`flat`（固定单价）或 `tiered`（阶梯累计），可配 input/output/cached 三类单价与折扣
- **预算池**决定管控：`exhaust_policy` = `block`（超支拒绝）/ `warn`（告警放行）/ `fallback`（自动降级）
- 后续改价只需新增费率卡规则——历史计量事件内嵌快照，改价不影响历史账单

### 4. 验证计费闭环

```bash
# 发起一次真实对话，然后：
curl -s "localhost:8080/api/v1/usage?customer_id=<UUID>" -H "$AUTH"          # 用量已累计
curl -s "localhost:8080/api/v1/budget-pools/<POOL_ID>/balance" -H "$AUTH"    # 预算已扣减
curl -sX POST localhost:8080/api/v1/billing-runs -H "$AUTH" \
  -d '{"period_start":"2026-08-23T00:00:00Z","period_end":"2026-08-24T00:00:00Z"}'  # 出账
curl -sX POST localhost:8080/api/v1/bills/<BILL_ID>/finalize -H "$AUTH"      # 定稿（不可再改）
```

### 5. 开启自动化调度

```bash
METERON_BILLING_INTERVAL=24h        # 自动出账（默认 0=手动触发）
METERON_RECON_LIGHT_INTERVAL=1h     # 小时级轻量对账（计量 vs 预聚合）
METERON_RECON_DEEP_INTERVAL=24h     # 每日深度对账（重算金额 vs 账本，差异自动补差）
```

### 6. 接入监控告警

`/metrics` 已暴露 Prometheus 指标，关键告警项：

| 指标 | 含义 | 建议告警 |
|------|------|----------|
| `reconciliation_diff_count` | 对账差异条数 | > 0 持续 2 个周期 |
| `budget_cache_drift_micros` | Redis-PG 预算缓存漂移 | 绝对值持续增长 |
| `budget_reservation_expired_total` | 过期预留（SSE 断连/超时） | 增速突增 |
| `gateway_overhead_seconds` | 网关附加延迟 | p99 > 100ms |

健康检查：`GET /healthz`。

### 7. 扩容与数据安全

- **网关多副本**：无状态可水平扩展；后台 Worker（出账/对账/过期释放）由 Redis 分布式锁保证多副本下单实例执行，扩副本无副作用
- **备份**：PostgreSQL（账本/账单，金融数据，建议每日全量+WAL 归档）；ClickHouse（计量明细，按需）；Redis 数据可重建（PG 为可信源），无需备份
- **Kafka**：分区数即 Sink 聚合并行度，事件按 `transaction_id` 分区保证同事务顺序

## 开发

```bash
go build ./...   # 构建
go test ./...    # 单元测试（计价引擎 / 预算状态机 / SSE 嗅探 / offset 提交语义）
```

模块结构：`internal/gateway`（在线代理）、`internal/ledger`（预算与账本）、`internal/pricing`（计价引擎）、`internal/meter`（计量聚合）、`internal/billing`（出账）、`internal/recon`（对账）、`internal/ui`（Web 控制台）。
