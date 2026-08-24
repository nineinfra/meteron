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
| **密钥管控** | API Key 支持绑定客户、scope 授权、过期时间、IP 白名单（CIDR）、模型白名单与启停，最小权限分发 |
| **渠道健康管理** | Provider 渠道错误率/延迟实时统计，超阈值自动降级切换 fallback，人工可恢复，指标暴露 Prometheus |
| **双重限流** | 客户维度 RPM/TPM/并发 + 上游 Provider 账号维度并发保护（Redis 滑动窗口），防打爆上游 |
| **审计与调试** | 请求全量审计日志多维查询（ClickHouse）；管理后台内置在线调试沙盒直发模型调用 |
| **数据自持** | 全栈自托管（PG / ClickHouse / Redis / Kafka），多租户 + RBAC 隔离 |

## 快速开始（2 条命令）

```bash
docker compose -f deploy/docker-compose.yml up -d --build   # 启动全栈 + Meteron（内置 mock provider，无需任何 API Key）
./scripts/verify.sh                                        # 端到端验证：建客户→对话→计量→出账→定稿→对账
```

> 验证脚本仅依赖 `bash` + `curl`，幂等可重复执行。服务就绪约需 30~60 秒（首次拉镜像较慢）。

## Web 控制台

API 与 Web 界面使用**独立端点**（便于分别暴露：控制台只对内网，API 端点对外服务）：

| 端点 | 端口 | 内容 |
|---|---|---|
| Web 控制台 | `:8080` | 浏览器访问的 UI（内置反向代理调用 API，同源无 CORS） |
| API/网关 | `:9080` | `/v1/chat/completions`、`/api/v1/*`、`/metrics`、`/healthz` |

启动后浏览器打开 **`http://localhost:8080/`**，输入 API Key（开发环境默认
`mk_meteron_bootstrap_dev_key`），左侧菜单三个界面：

- **监控看板**：KPI 卡片（请求数、输入/输出/缓存 tokens，24h/7d/30d 切换）、用量趋势图、
  按模型聚合的用量明细、预算池实时余额与进度条、账单管理（draft 一键定稿）
- **模型对话**：与模型流式聊天（支持思考模型 reasoning 展示），每轮显示 token 用量，
  对话产生的用量实时进入监控看板
- **管理**：API Key 管理（创建即见明文、IP/模型白名单、过期、启停）、渠道健康
  （错误率/延迟/降级状态、人工恢复）、审计日志多维查询、在线调试沙盒

控制台为内嵌静态资源（同源调用 Management API，无外部 CDN 依赖），监控数据每 60 秒自动刷新。

## 30 秒手工体验

自动验证之外，核心闭环手工只需三步（`KEY` 为引导 API Key）：

```bash
# 1. 建客户（拿到 customer_id）
curl -sX POST localhost:9080/api/v1/customers \
  -H "Authorization: Bearer mk_meteron_bootstrap_dev_key" \
  -H "Content-Type: application/json" \
  -d '{"name":"Acme","external_id":"acme","currency":"USD"}'

# 2. 发起对话（默认模型为本机 Ollama 的 qwen3.5:9b；X-Meteron-Customer-ID 支持 UUID 或 external_id）
curl -sX POST localhost:9080/v1/chat/completions \
  -H "Authorization: Bearer mk_meteron_bootstrap_dev_key" \
  -H "X-Meteron-Customer-ID: acme" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen3.5:9b","messages":[{"role":"user","content":"hello"}]}'

# 3. 查用量（异步链路 Kafka→ClickHouse，约 5 秒后可查；customer_id 需用第 1 步返回的 UUID）
curl -s "localhost:9080/api/v1/usage?customer_id=<第1步返回的id>" \
  -H "Authorization: Bearer mk_meteron_bootstrap_dev_key"
```

计费、预算、对账是同一套数据的后台加工：`/api/v1/budget-pools`（预算池）、`/api/v1/billing-runs`（出账跑批）、`/api/v1/reconciliation-runs`（对账结果）。

## 系统架构

### 对外 API 接口（集成视图）

应用与外部系统通过以下接口将 Meteron 集成到自己的体系中（详细参数见 `scripts/verify.sh`）：

![对外 API 接口（集成视图）](docs/integration-view.svg)

> 图源文件为 [docs/integration-view.svg](docs/integration-view.svg)（纯文本 SVG，可直接编辑）。

> 所有业务接口鉴权：`Authorization: Bearer <api-key>`；端点：API/网关 `:9080`，Web 控制台 `:8080`。

### 内部数据流

![内部数据流](docs/data-flow.svg)

> 图源文件为 [docs/data-flow.svg](docs/data-flow.svg)（纯文本 SVG，可直接编辑）。

图中方框为实体（应用 / Meteron / LLM Provider / Redis / PostgreSQL / Kafka / ClickHouse），带编号箭头为步骤及流转的数据。箭头方向 = 数据流转方向（起点为读取源，终点为写入目标），步骤执行者标注在箭头标签中：①–④ 由 Meteron 网关同步执行（毫秒级）；⑤–⑨ 由 Meteron 进程内后台 Worker（relay / sink / billing / recon）异步执行，不阻塞请求。Meteron 框以三层错位叠加表示多副本部署：网关无状态、可水平扩容；后台 Worker 经 Redis 分布式锁保证多副本下仅单实例执行。

- **① 请求接入**：应用以 OpenAI 兼容协议调用网关，只改 `base_url` 与鉴权头；网关完成 API-Key 鉴权（HMAC-SHA256 哈希等值查找）、IP/模型白名单与客户/渠道双重限流校验，并按三种模式路由（显式 `X-Meteron-Provider` 指定渠道 / `model=auto` 成本分级自动路由 / 常规 route_rules，详见「2.5 渠道路由」）。
- **② 预算预留**：网关按 `max_tokens` 估算金额，Redis 原子扣减预算缓存（快路径）+ PG 预留表落库复核（可信源）两阶段预留；超额按策略 block/warn/fallback，请求结束按实际用量 confirm（差额归还）或 release。
- **③ 转发上游**：网关将请求透明转发至 LLM Provider（OpenAI / Anthropic / GLM / 本地模型 / mock）；路由或降级切换模型时同步重写请求体 `model` 字段，保证计费模型与实际执行模型一致。
- **④ 响应与 usage 嗅探**：网关边透传响应边解析 token 用量——非流式解析 JSON，流式逐帧解析 SSE（自动注入 `stream_options.include_usage`）；计费收尾脱离请求生命周期，客户端断开也不丢账。
- **⑤ 计量事件落 Outbox**：网关产出内嵌费率快照的 MeterEvent（改价不影响历史账单），事务写入 PG Outbox 表持久化（崩溃不丢事件），并 confirm 预算预留。
- **⑥ 事件投递**：Meteron 进程内 relay Worker 从 PG Outbox 捞取未发布事件投递 Kafka（`meter-events` 主题，按 `transaction_id` 分区保序），成功标记 `published_at` 并定期清理；Kafka 不可用不阻塞在线链路。Outbox 仅作可靠性中转——PG 只短暂保留近期事件（已发布即清理），全量计量明细长期存 ClickHouse。
- **⑦ 消费落库**：Meteron 进程内 sink Worker 消费 Kafka 写入 ClickHouse 计量明细（写成功才提交 offset，至少一次语义 + `event_id` 幂等去重）；聚合器按小时桶预聚合，offset 只提交已落盘小时的连续前缀。
- **⑧ 出账**：Meteron 进程内 billing Worker 执行 BillingRun，从 ClickHouse 读取明细按费率快照计价（跨档按累计 token 切分），生成 draft 账单 + 账单项 + 不可变账本流水写入 PG；finalize 后账单不可变，跑批幂等可安全重跑。
- **⑨ 对账**：Meteron 进程内 recon Worker 每小时轻量对账（明细 vs 预聚合）、每日深度对账（ClickHouse 重算金额 vs PG 账本流水合计），差异自动生成 correction 流水写回 PG——多记红冲、少记补差，账本只增不改。

## 生产级使用步骤

按顺序执行即可从验证环境升级为生产环境。

### 1. 密钥加固（必做）

```bash
# 生成 32 字节主密钥（API Key 哈希的 HMAC pepper）
openssl rand -hex 32
```

在部署环境注入并重启（容器部署全部走环境变量，见 deploy/docker-compose.yml）：

```bash
METERON_AES_KEY=<新key>                  # API Key 单向哈希（HMAC-SHA256）的 pepper，泄漏=API Key 可被离线穷举
METERON_BOOTSTRAP_API_KEY=<强随机值>     # 首次启动自动创建为 admin 权限密钥
```

> **Key 存储**：`api_keys.key_hash` 保存 HMAC-SHA256 单向哈希（不落明文、不可逆），
> 主密钥仅作为哈希 pepper；升级部署时存量引导密钥会在启动时自动迁移为单向哈希。
> 引导 Key 即当前的管理入口（admin 全权限）。轮换方式：改环境变量重启会在原租户下
> 自动新增一条 key 记录，旧 key 通过数据库禁用（`UPDATE api_keys SET deleted_at=NOW() WHERE ...`）。

**最小权限密钥**（推荐替代直接分发引导 Key）：

```bash
# 创建绑定客户、30 天过期、仅网关调用、限 IP 与模型的密钥（明文仅此一次返回）
curl -sX POST localhost:9080/api/v1/api-keys -H "$AUTH" -d '{
  "customer_id": "<UUID>",
  "scopes": ["gateway_invoke"],
  "expire_at": "2026-09-23T00:00:00Z",
  "ip_whitelist": ["10.0.0.0/8"],
  "model_allow_list": ["gpt-4o-mini"]
}'
# 启停与吊销
curl -sX PATCH localhost:9080/api/v1/api-keys/<ID>/enabled -H "$AUTH" -d '{"enabled":false}'
curl -sX DELETE localhost:9080/api/v1/api-keys/<ID> -H "$AUTH"
```

密钥级管控在网关链路强制执行：IP 白名单 fail-closed、模型白名单校验最终转发模型
（含 fallback 调整后）、过期/禁用即时失效。

### 2. 接入真实 Provider

`config.yaml` 配置后重启（Provider 的 `api_key` 仅存在于配置/环境变量中，不落库）：

```yaml
providers:
  - name: openai-main
    type: openai            # openai | openai-compatible | local | anthropic | mock
    base_url: "https://api.openai.com/v1"
    api_key: "sk-..."
    models: ["gpt-4o", "gpt-4o-mini"]
    cost_tier: premium      # 成本分级：economy | standard | premium（auto 路由按此选渠道）
  - name: zhipu
    type: openai            # GLM 兼容模式同样走 openai 类型
    base_url: "https://open.bigmodel.cn/api/paas/v4"
    api_key: "..."
    models: ["glm-4.6"]
    cost_tier: standard
  - name: ollama            # 本地部署模型：Ollama / vLLM / LM Studio 等 OpenAI 兼容端点，api_key 可留空
    type: local
    base_url: "http://localhost:11434/v1"
    models: ["llama3.1", "qwen2.5:7b"]
    cost_tier: economy
```

### 2.5 渠道路由：显式指定 / auto 自动 / 成本分级策略

应用接入后每个请求有三种路由模式（按优先级）：

| 模式 | 用法 | 行为 |
|---|---|---|
| **显式指定渠道** | 请求头 `X-Meteron-Provider: zhipu` | 锁定该渠道（跳过路由规则）；`model` 具体时直接透传，`model=auto` 时取该渠道配置序首个模型 |
| **auto 自动路由** | 请求体 `"model": "auto"` | Meteron 按请求复杂度评估选择渠道与模型（见下方策略） |
| **常规路由** | 请求体 `"model": "gpt-4o"` | `route_rules` 任务类型规则优先，未命中时按模型反查渠道 |

**auto 模式的成本分级策略**——复杂度低/要求低的请求走本地或低成本渠道，复杂度高/要求高的请求走高能力渠道：

```bash
# 简单问答（输入 < 2000 token 估算）→ economy：本地 Ollama / 国内低成本模型
curl -X POST localhost:9080/v1/chat/completions -H "$AUTH" \
  -H "X-Meteron-Customer-ID: $CID" -H "X-Meteron-Task-Type: chat" \
  -d '{"model":"auto","messages":[{"role":"user","content":"你好"}]}'

# 复杂任务（多模态/工具调用/长输入 ≥ 8000 token）→ premium：GPT-4o / Claude
curl -X POST localhost:9080/v1/chat/completions -H "$AUTH" \
  -H "X-Meteron-Customer-ID: $CID" -H "X-Meteron-Task-Type: chat" \
  -d '{"model":"auto","tools":[...],"messages":[{"role":"user","content":"分析这张图"}]}'

# 显式质量要求（覆盖启发式）：低要求强制 economy，高要求强制 premium
curl ... -H "X-Meteron-Quality: low"  -d '{"model":"auto",...}'
```

复杂度评估信号（优先级从高到低）：

1. **`X-Meteron-Quality` 头**：`low` / `medium` / `high` → `economy` / `standard` / `premium`（显式指定，覆盖启发式）
2. **多模态或工具调用**（消息含 `image_url`、请求带非空 `tools`）→ `premium`
3. **输入规模**（token 估算）：`≥ 8000` → `premium`，`≥ 2000` → `standard`，否则 `economy`

成本策略的精细控制通过 `route_rules.policy_config` 的 `auto` 字段（分级阈值与各级目标渠道均可覆盖）：

```json
{
  "budget_watermark": 0.2,
  "auto": {
    "standard_min_input_tokens": 1000,
    "premium_min_input_tokens": 4000,
    "tiers": {
      "economy":  {"provider": "ollama", "model": "qwen2.5:7b"},
      "standard": {"provider": "zhipu",  "model": "glm-4.6"},
      "premium":  {"provider": "openai-main", "model": "gpt-4o"}
    }
  }
}
```

- 未配置 `tiers` 时按渠道 `cost_tier` 分级反查（配置序优先）
- 目标分级无可用渠道时向低成本方向降级（premium→standard→economy），可用性优先
- 预算水位低于 `budget_watermark` 时 auto 结果同样向低成本分级降一级（省钱优先）
- `economy` 分级同时也是 auto 结果的 fallback（渠道健康降级、预算降级复用现有机制）

> 注意：auto 路由出的最终模型仍受密钥模型白名单校验——使用 auto 模式的密钥需在白名单中放行所有允许路由到的模型。

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
curl -s "localhost:9080/api/v1/usage?customer_id=<UUID>" -H "$AUTH"          # 用量已累计
curl -s "localhost:9080/api/v1/budget-pools/<POOL_ID>/balance" -H "$AUTH"    # 预算已扣减
curl -sX POST localhost:9080/api/v1/billing-runs -H "$AUTH" \
  -d '{"period_start":"2026-08-23T00:00:00Z","period_end":"2026-08-24T00:00:00Z"}'  # 出账
curl -sX POST localhost:9080/api/v1/bills/<BILL_ID>/finalize -H "$AUTH"      # 定稿（不可再改）
```

### 5. 开启自动化调度与保护性限流

```bash
METERON_BILLING_INTERVAL=24h        # 自动出账（默认 0=手动触发）
METERON_RECON_LIGHT_INTERVAL=1h     # 小时级轻量对账（计量 vs 预聚合）
METERON_RECON_DEEP_INTERVAL=24h     # 每日深度对账（重算金额 vs 账本，差异自动补差）

# 双重限流（全部 0=不限制；Redis 滑动窗口，限流器故障 fail-open 放行）
METERON_RATE_LIMIT_CUSTOMER_RPM=60          # 客户每分钟最大请求数
METERON_RATE_LIMIT_CUSTOMER_TPM=100000      # 客户每分钟最大 token 数
METERON_RATE_LIMIT_CUSTOMER_MAX_CONCURRENT=10   # 客户最大并发
METERON_RATE_LIMIT_PROVIDER_MAX_CONCURRENT=20   # 上游渠道单账号最大并发（防打爆上游）

# 渠道健康探测与自动降级
METERON_HEALTH_PROBE_INTERVAL=60s     # 主动探测间隔（GET base_url，0=关闭）
METERON_HEALTH_WINDOW=5m              # 错误率统计滑动窗口
METERON_HEALTH_ERROR_RATE_THRESHOLD=0.5  # 自动禁用阈值（含真实流量与探测样本）
METERON_HEALTH_MIN_SAMPLES=10         # 触发降级最小样本数（防小样本误杀）
```

渠道错误率超阈值自动禁用，路由引擎切换 fallback（均不可用则拒绝并释放预留）；
人工恢复 `POST /api/v1/providers/{name}/enable`（重置统计重新观察）。

### 6. 接入监控告警

`/metrics` 已暴露 Prometheus 指标，关键告警项：

| 指标 | 含义 | 建议告警 |
|------|------|----------|
| `reconciliation_diff_count` | 对账差异条数 | > 0 持续 2 个周期 |
| `budget_cache_drift_micros` | Redis-PG 预算缓存漂移 | 绝对值持续增长 |
| `budget_reservation_expired_total` | 过期预留（SSE 断连/超时） | 增速突增 |
| `gateway_overhead_seconds` | 网关附加延迟 | p99 > 100ms |
| `provider_health_error_rate` | 渠道滑动窗口错误率 | > 0.3 持续 2 个周期 |
| `provider_health_avg_latency_seconds` | 渠道平均延迟 | 环比突增 3 倍 |

健康检查：`GET /healthz`。

### 7. 扩容与数据安全

- **网关多副本**：无状态可水平扩展；后台 Worker（出账/对账/过期释放）由 Redis 分布式锁保证多副本下单实例执行，扩副本无副作用
- **备份**：PostgreSQL（账本/账单，金融数据，建议每日全量+WAL 归档）；ClickHouse（计量明细，按需）；Redis 数据可重建（PG 为可信源），无需备份
- **Kafka**：分区数即 Sink 聚合并行度，事件按 `transaction_id` 分区保证同事务顺序

## 开发

```bash
go build ./...   # 构建
go test ./...    # 单元测试（计价引擎 / 预算状态机 / SSE 嗅探 / offset 提交语义 / outbox 投递
                 #   / API Key 哈希与白名单 / 双重限流滑动窗口 / 渠道健康降级 / 审计查询构造 / 调试沙盒）
```

模块结构：`internal/gateway`（在线代理）、`internal/router`（路由策略）、`internal/ledger`（预算与账本）、`internal/pricing`（计价引擎）、`internal/meter`（计量聚合）、`internal/billing`（出账）、`internal/recon`（对账）、`internal/ingest`（事件与 outbox 投递）、`internal/health`（渠道健康）、`internal/ratelimit`（双重限流）、`internal/ui`（Web 控制台）。
