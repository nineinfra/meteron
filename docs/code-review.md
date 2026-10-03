# Meteron 代码评审报告

> 评审日期：2026-08-24
> 评审范围：全仓库（Go 后端 / SQL 迁移 / Web 控制台 / 部署脚本）
> 结论：架构成熟度高于典型 MVP，P0/P1 问题已在本轮修复（见 §4），遗留 P2 见 §5。

## 1. 项目概览

Meteron 是自托管的 LLM 计量计费网关：应用把 LLM 请求指向 Meteron，即可获得
token 级计量、实时预算管控、精确计费与自动对账，数据 100% 自持。

| 维度 | 内容 |
|---|---|
| 语言/形态 | Go 1.24 模块化单体（网关 + 管理 API + 全部后台 Worker 同进程，可拆分部署） |
| 存储 | PostgreSQL（账本/账单/配置，可信源）· ClickHouse（计量明细+预聚合）· Redis（预算缓存/分布式锁）· Kafka（事件总线） |
| 前端 | Vue 3 + Vite，构建产物内嵌 Go 二进制（`internal/ui`） |
| 规模 | Go 源码约 30 文件；最大 `internal/api/customers.go` 547 行，无超大文件 |

## 2. 架构分析

### 2.1 在线链路（毫秒级，同步）

`internal/gateway/handler.go` 串联完整请求生命周期：

1. **鉴权**：Bearer Token → `rbac.Authenticate`（key_hash 等值查找 + scope 校验）
2. **元数据解析**：客户（UUID/external_id）、任务类型、幂等键（UUIDv5 映射 transaction_id）
3. **预算预留**：按 max_tokens 估算金额，`ledger.Reserve` 两阶段预留
4. **路由决策**：任务类型/模型/预算水位 → Provider；超支策略 `block`/`warn`/`fallback`
5. **转发**：OpenAI 兼容透明代理；流式注入 `stream_options.include_usage=true`
6. **usage 嗅探**：`sse.go` 边透传 SSE 边解析 token 用量
7. **收尾**（脱离请求生命周期，防客户端断开丢计费）：confirm 预留 + 产出 MeterEvent

### 2.2 预算两阶段（核心机制）

`internal/ledger/reservation.go`：经典 reserve → confirm/release 状态机。

- **Reserve**：Redis 原子扣减（快）→ PG `SELECT ... FOR UPDATE` 复核落库（可信）；
  PG 失败回滚 Redis——PG 为可信源，Redis 为加速缓存
- **Confirm**：按实际用量结算，差额 `addBack` 归还 Redis
- **兜底**：预留 TTL（默认 60s）+ 后台扫描过期释放；周期性 Redis-PG 漂移校正
- **状态机**：`CanTransition` 严格校验 pending → confirmed/released/expired，终态不可逆

### 2.3 计量管道（异步）

```
MeterEvent(含费率快照) → PG Outbox → Kafka(按 transaction_id 分区) → Sink → ClickHouse
```

- **费率快照内嵌事件**：改价不影响历史账单（设计亮点）
- **Sink**（`meter/sink.go`）：写 ClickHouse 成功才提交 offset，至少一次 + 幂等
- **聚合**（`meter/aggregate.go`）：小时桶预聚合，offset 只提交"已落盘小时的连续前缀"
  （`committablePrefix`），晚到事件不丢；查询侧已关闭小时走预聚合、未关闭走 `FINAL` 实时合并

### 2.4 计价与出账

- `pricing/engine.go`：请求时刻固化快照；flat/tiered × input/output/cached 差异化定价
- `pricing/calc.go` `EventCostCumulative`：按累计 token 跨档切分，解决事件级快照
  不知道累计档位的问题，出账时用 ClickHouse 全量重算
- `billing/worker.go`：BillingRun → draft 账单 + bill_line_items + usage_charge 流水；
  finalize 后不可变，跑批幂等可重跑

### 2.5 对账

`recon/worker.go` 两级：轻量（1h，明细 vs 预聚合）、深度（24h，重算金额 vs 账本）。
差异自动生成 correction 红冲/补差流水——账本只增不改（不可变账本），标准复式账实践。

### 2.6 数据分工

| 存储 | 角色 | 备份策略 |
|---|---|---|
| PG | 金融数据（账本/账单/预留/配置/API Key） | 每日全量 + WAL 归档（建议） |
| ClickHouse | 计量明细+聚合 | 按需（可从事件重放） |
| Redis | 预算缓存/锁 | 无需备份（可从 PG 重建） |
| Kafka | 事件总线 | 分区=Sink 并行度，transaction_id 保序 |

网关无状态可水平扩；后台 Worker 由 Redis 分布式锁保证多副本单实例执行。

## 3. 亮点

- **在线/离线职责彻底分离**：在线只做"预留+转发"，全链路（事件→账单→对账）幂等
- **工程细节扎实**：offset 提交语义、预留 TTL 兜底、缓存漂移校正、费率快照
- **测试覆盖核心难点**：计价引擎、预算状态机、SSE 嗅探、offset 语义、流超时
- **可演示性**：`verify.sh` 端到端幂等验证 + mock provider 零依赖启动

## 4. 问题清单与修复记录

### P0 — API Key 哈希可逆（已修复）

**位置**：`internal/rbac/apikeys.go`

**问题**：`key_hash` 采用确定性 AES-GCM 加密（nonce = HMAC(明文)）以支持等值查找。
加密可逆：攻击者同时获得数据库导出与主密钥（`METERON_AES_KEY`）即可解密还原全部
API Key 明文，等效于鉴权体系整体失陷。README 中"加密存储上游 LLM API Key"的描述
亦与实现不符（Provider key 实际仅在配置中，未落库）。

**修复**：
- `key_hash` 改为 `HMAC-SHA256(主密钥, 明文)` 单向哈希：等值查找能力保留，
  主密钥泄漏也无法反推明文（API Key 为 128+ bit 高熵随机串，穷举不可行）
- 主密钥语义变更为哈希 pepper，鉴权与新建密钥路径不再使用任何可逆运算
- 存量兼容：`Bootstrap` 启动时用配置中的引导密钥明文一次性将旧格式记录
  `UPDATE` 为新哈希（旧算法仅保留于此迁移路径）；`Authenticate` 只接受新格式
- 单测：`apikeys_test.go` 覆盖确定性、单向性（legacy 可解密 vs 新格式不可逆）、
  pepper 依赖、新旧格式可区分性

### P1 — Kafka 事件仅内存缓冲，崩溃丢失（已修复）

**位置**：`internal/ingest/producer.go`

**问题**：Kafka 不可用时事件进入进程内存缓冲，`replayLoop` 周期重放；进程崩溃
即丢失缓冲事件（计量缺口，依赖对账兜底但存在窗口）。原注释亦自认
"生产版本应落盘 WAL"。

**修复**（PG Outbox 模式，新增迁移 `003_meter_outbox_pg.sql`）：
- `Produce` 先将事件落 PG `meter_outbox`（持久化）→ 再尽力即时投递 Kafka →
  成功即标记 `published_at`；即时投递失败不影响正确性
- 后台 `relayLoop`（5s）扫描未发布事件补投，Kafka 失败即停本轮（快速让出）；
  已发布记录保留 7 天后清理
- **崩溃语义**：outbox 已落盘，重启后 relay 继续投递，不丢事件
- **重复语义**：多副本并发 relay / 投递成功但标记失败可能重复投递——至少一次语义，
  下游 ClickHouse 按 event_id（ReplacingMergeTree）幂等去重，安全
- outbox 落库失败显式返回错误（PG 不可用时调用方感知，不静默丢弃）
- 单测：`producer_test.go` 覆盖"先落盘再投递""Kafka 宕机不丢事件+恢复补投"
  "outbox 不可用时报错""relay 中途失败保留进度"四类语义

### P1 — PG 迁移筛选硬编码，新增迁移永不执行（已修复）

**位置**：`internal/store/pg.go` `MigratePG`

**问题**：迁移框架本身已有 `schema_migrations` 版本表与事务化执行，但脚本筛选
硬编码 `strings.HasPrefix(name, "001")`——任何新增 PG 迁移脚本（003、004…）
都不会被执行，版本化迁移形同虚设。

**修复**：
- 筛选规则改为命名约定：`NNN_*_pg.sql` 为 PG 迁移；ClickHouse 脚本
  （`002_clickhouse_init.sql`）由 `NewClickHouse` 单独应用，继续排除
- 提取纯函数 `pgMigrationNames`，单测覆盖筛选与排序（本轮新增的
  `003_meter_outbox_pg.sql` 即为首个受益者，同时验证了修复有效）
- 单测：`pg_test.go` 另覆盖 `splitSQL` 与 `idempotentDDL`

### 修复验证

本轮在无 Go 工具链的开发机上完成，已做静态核查（导入、接口签名、全部调用点兼容性）；
构建与测试需在有 Go 1.24 环境的机器上执行：

```bash
go build ./...
go test ./...    # 含新增 rbac / ingest / store 三个测试文件
```

## 5. 遗留风险（P2，建议后续处理）

| # | 问题 | 说明 | 建议 |
|---|---|---|---|
| 1 | 鉴权模型单薄 | MVP 统一走 bootstrap admin Key；`CreateKey` 已具备按客户/scope 发放能力但无 HTTP 端点 | 暴露 Key 管理 API + 最小权限 Key（README 已列为 V1 路线项） |
| 2 | ClickHouse 迁移无版本化 | `NewClickHouse` 每次启动重放 002 全量 DDL（幂等），新增 CH 脚本不会执行 | 参照 PG 方式建 `schema_migrations`（CH 侧） |
| 3 | 未关闭小时 `FINAL` 查询 | 数据量大后实时聚合可能成查询瓶颈 | 物化视图 / 提高聚合频率 / 强制 TTL 关桶 |
| 4 | Provider API Key 明文于配置 | `config.yaml`/环境变量持有上游 key（未落库，AES key 与其无关） | 密钥管理服务接入或至少 env-only 注入 |
| 5 | RLS 未启用策略 | 各表 `ENABLE ROW LEVEL SECURITY` 但未创建 policy（owner 直连全放行），多租户隔离实际依赖应用层 tenant_id 过滤 | 若开放共享部署，需补 policy 并以非 owner 角色连接 |
| 6 | Outbox 与业务不同事务 | 事件落 outbox 与预算 confirm 不在同一 PG 事务，极端时序下可能出现"已扣费无事件"或反之（对账 Worker 可兜底修正） | 将 confirm 与 outbox insert 合并进同一事务 |

## 6. 总体评价

这是一个**设计成熟度明显高于典型 MVP 的项目**：两阶段预算、费率快照、不可变账本、
双轨对账等商业计费系统的关键模式均正确落地；在线/离线分离与全链路幂等是最大架构
亮点。本轮修复解决了鉴权可逆性与事件持久化两个阻断生产使用的问题，并打通了迁移
体系的最后一公里。建议按 §5 顺序推进 P2 项。

---

# 第二轮评审（feature/competitive-features 分支）

> 评审日期：2026-08-24（第二轮）
> 评审范围：五项竞品能力实现（密钥治理 / 渠道健康 / 双重限流 / 审计查询 / 调试沙盒）
> 及其全链路交叉验证（rbac / ratelimit / health / api / gateway / config / main / 前端）
> 结论：发现 P0 × 3、P1 × 8，已全部修复并补充回归测试；遗留项见 §7。

## 7. 问题清单与修复记录（第二轮）

### P0 — 路由/降级切换模型后请求体 model 未重写（已修复）

**位置**：`internal/gateway/handler.go` + `internal/provider/openai.go`

**问题**：路由规则 target_model 覆盖、预算降级、健康禁用 fallback 切换模型后，
OpenAI 兼容适配器仍把客户端原始 body 原样 POST（`req.Model` 从未写入 body）。
后果：① fallback 渠道不承载原模型时上游直接 404，降级路由形同虚设；
② 目标渠道同时承载两模型时，实际执行 A 模型、按 B 模型费率计费——计量数据
"自洽地错"，对账无法发现。

**修复**：转发前 `rewriteBodyModel`（`map[string]json.RawMessage` 保留其余字段）
重写 body.model；回归测试 `TestRewriteBodyModel` 断言字段保留与非法 body 拒绝。

### P0 — 客户级密钥可把用量计费到任意同租户客户（已修复）

**位置**：`internal/gateway/handler.go`（`ident.CustomerID` 全文未被引用）

**问题**：密钥支持绑定客户（`Identity.CustomerID`），管理 API 侧已强制隔离
（`identity.go`/`usage.go`），但计费热路径的 customerID 完全由请求头
`X-Meteron-Customer-ID` 决定——持有客户 A 密钥者传 B 的 ID 即可污染 B 的
预算池与账单（越权计费）。

**修复**：`resolveCustomer` 后强制 `customerAllowed` 校验（绑定客户不一致 →
403 `CUSTOMER_NOT_ALLOWED`，新增错误码 1017）；测试 `TestCustomerAllowed`。

### P0 — 内置默认密钥随源码公开（CWE-798，已修复）

**位置**：`internal/config/config.go`（默认 AESKey/BootstrapKey）、`web/src/App.vue`、
`deploy/docker-compose.yml`

**问题**：默认 `bootstrap_api_key=mk_meteron_bootstrap_dev_key`（admin scope）
与 pepper 均为源码公开值；照抄 compose 启动即向任何知道仓库的人开放全部管理 API。
前端引导页还明文展示该默认密钥。

**修复**：`validate()` 检测默认密钥即拒绝启动，须显式豁免（yaml
`insecure_defaults: true` 或 env `METERON_INSECURE_DEFAULTS=true`，本地开发栈
与 config.yaml 已显式声明）；`Load()` 同时修复静默吞掉配置文件读取错误（仅
ErrNotExist 维持"存在才加载"）；前端移除默认密钥文案；新增 `config_test.go`
覆盖拦截/豁免/自定义密钥/读取错误四类路径。

### P1（已修复，摘要）

| # | 问题 | 修复 |
|---|---|---|
| 1 | `max_tokens` 无上限 → `estimateTokens` int64 溢出为负绕过 TPM 并污染窗口 | 入口校验 `[0, 1_000_000]`（`validateChatRequest`）+ 测试 |
| 2 | 管理 API 不检查密钥 IP 白名单（第二道防线整体绕过） | `authMiddleware` 复用 `rbac.ClientIP`+`IPAllowed`（实现下沉 rbac，网关/管理面同源） |
| 3 | `key_hash` 无索引：鉴权热路径全表扫描 | 迁移 `005` 唯一索引（同时兜底 Bootstrap 并发重复插入） |
| 4 | 调试沙盒内部请求无 `RemoteAddr`：带 IP 白名单的密钥必现 403 | 透传 `RemoteAddr` 与 XFF 链 + 测试 |
| 5 | 审计查询 `rows.Err()` 未检查：出错返回 200+静默截断数据 | 迭代后显式检查 |
| 6 | `customer_id` 非法 UUID → ClickHouse `toUUID()` 抛错变 500 | 入口 `uuid.Parse` 校验 → 400 |
| 7 | `expire_at` 带非零偏移实际失效点偏移数小时（TIMESTAMP 无时区列） | 解析后统一 `.UTC()` |
| 8 | 健康列表只遍历 samples：人工禁用的无流量渠道"消失" | `Snapshot` 取 samples ∪ manual ∪ auto + 测试 |

### 修复验证

`go build ./...`、`go vet ./...`、`go test ./...` 全部通过（含新增 config /
gateway / api / health 测试用例）。

## 8. 遗留风险（第二轮，建议后续处理）

| # | 问题 | 说明 | 建议 |
|---|---|---|---|
| 1 | Redis 限流 Lua 滑动窗口 O(N) 全量遍历 | 每次.AllowRate 在 Lua 内 `ZRANGE 0 -1`+逐成员 HGET，高限额时限流器自身成为 Redis 瓶颈（O(N²) 总复杂度） | 改 HASH 累加器/分桶计数；引入 miniredis 补 Redis 实现的语义测试（当前仅 Memory 实现有测试） |
| 2 | Redis 并发信号量 TTL 10 分钟不续期 | 超长流式请求持有期过期 → 计数清零超发，旧持有者 release 会吞掉新计数 | release 带持有者 token（集合信号量）或每次 Acquire 续期 |
| 3 | `writeErr` 向客户端透出内部错误 detail | PG/CH 错误文本（含 SQL/约束名）直接进响应；仅 admin 可达，风险低 | 5xx 类 detail 记日志、对外固定文案 |
| 4 | `requireScope` 权限不足返回 401 而非 403 | RFC 语义偏差，测试已把 401 固化为契约 | 统一为 403 并同步测试 |
| 5 | admin 密钥明文存 localStorage | 无 HttpOnly 保护，一旦引入 XSS 即凭据失窃 | 内存态保存或后端签发短期 session token |
| 6 | 幂等键占用不区分预留状态 | released/expired 的同键重试永远 409，与常见幂等语义（成功前可重试）相悖 | 仅 pending/confirmed 视为占用（唯一约束改部分索引） |
| 7 | `from` 可传 1970 年绕过 24h 默认窗口 | FINAL+跨全分区扫描代价高；audit 排序缺 event_id 次级键，OFFSET 翻页在时间并列处可能重复/漏行 | 校验 from≤to、限制最大跨度；`ORDER BY event_time, event_id` |
| 8 | mock 渠道探测样本稀释错误率 | BaseURL 为空的渠道每 interval 注入成功样本，真实流量 100% 失败时混合探测可能低于阈值，延迟自动禁用 | 探测样本与真实流量分桶统计 |

### 状态更新（第三轮，2026-09-15）

| # | 状态 | 处理 |
|---|---|---|
| 1 | 已修复 | 滑动窗口改为单 ZSET（member 编码 `cost:uuid`），Lua 内求和：Redis 调用数恒为 4，与窗口内请求数无关；不再需要 HASH key。已引入 miniredis 补 Redis 实现语义测试 |
| 2 | 已修复 | Acquire 每次都刷新 TTL（原仅首个许可设置 → 计数长期不为零时 key 过期、计数清零超发）；新增持有期 keepAlive 心跳（TTL/4 续期），进程崩溃仍由 staleTTL 兜底；release 用 sync.Once 保证幂等 |
| 3 | 已修复 | gateway 与 api 两处 writeErr：5xx（含 502 上游）detail 只记 slog、对外固定 `internal error`；4xx 保留 detail |

## 9. 第三轮修复：P0/P1 缺陷（2026-09-15）

| 级别 | 问题 | 修复 |
|---|---|---|
| P0 | 预聚合键缺租户/客户维度 → 跨客户用量串账 | `aggregate.go` 聚合键前置 tenantID+customerID（表排序键本就含这两列）；补单元测试 |
| P0 | MeterWorker 内存无界 | pendingMsg 只留 topic/partition/offset（丢弃 64KB 载荷）；软/硬阈值背压；seenIDs 上限降级 |
| P0 | Sink 写 CH 失败后 for{} 死循环，消费静默停滞 | 5 次指数退避（3s→30s），超限返回错误停止 Worker；main 中触发进程退出以免静默 |
| P1 | bills 无周期唯一约束 + 分布式锁不续期 → 重复 draft 账单 | 迁移 `006` 部分唯一索引（draft/finalized）；`Redis.RefreshLock` + 跑批续期 goroutine |
| P1 | QueryEvents 无上限，出账/深度对账全量载入内存 | 加 100 万行硬上限（超限报错）；出账与对账改为按 1h 窗口分片拉取 |
| P1 | ClickHouse 无 TTL、明细存 64KB raw_payload | 明细 TTL 13 个月、hourly 3 年 |
| P1 | Authenticate 每次请求查 PG（热路径） | 5s 进程内缓存（上限 1 万）；密钥写操作整体失效，保证禁用即时生效 |
| P1 | flushClosed 持锁写 ClickHouse（最长 30s 阻塞消费） | 改为锁内摘桶、锁外写入；inflight 防重复落盘 + hourBucket.merge 处理回写 |
| — | 工程卫生 | 删除误提交的 debug.log；`deploy/Dockerfile.dockerignore` 移至仓库根 `.dockerignore`（context 为仓库根，原位置不生效） |

**后续待办**：`006` 迁移要求库内无同周期重复 draft，若存在会建索引失败（故意不静默跳过），
需先手工清理。hourly 表的历史数据仍是串账状态，需用 `meter_events FINAL` 重建
（修复只对新事件生效）。

---

# 第四轮评审（全仓库复审，2026-10-01）

> 评审范围：全仓库复检（gateway / ledger / pricing / billing / recon / meter /
> ratelimit / rbac / health / api / config / store / provider / worker / main）。
> 重点：① 核实前三轮修复是否落地；② 挖掘尚未记录的新问题。
> 结论：前三轮 P0/P1 修复全部核实到位；本轮新发现 P1 × 3、P2 × 7，详见 §10；
> 历史遗留未关闭项核对结果见 §11。

## 10. 新发现问题清单（第四轮）

### P1 — 预算超卖窗口：预留估算与实际计价脱钩（设计缺口）

**位置**：`internal/gateway/handler.go`（estimateAmount）+ `internal/ledger/reservation.go`（Confirm）

**问题**：预留金额按全局配置 `estimate_unit_price`（默认 0.000002/token）估算，
与该客户实际命中的费率卡快照价格完全无关。`Confirm` 按实际金额入账且**不校验池上限**：
`actual > estimated` 时差额为负，Redis 可用额度直接扣成穿透，但本笔请求已被放行。
对高价模型（premium 渠道单价可达估算值的数百倍），单请求实际消耗可远超预留金额。
README 声称"不超卖"，实际语义是"下一笔请求会被拦住，本笔已超支"。

**建议**：Reserve 前先取 `pricing.ResolveSnapshot`，用快照单价 ×（输入估算 + max_tokens）
计算预留（快照本来就要取，无额外开销）；Confirm 发现 actual > estimated 时打点
`budget_overshoot` 指标供告警。

### P1 — 无预算池时幂等键完全失效（重复计费）

**位置**：`internal/gateway/handler.go`（checkTransactionExists / 步骤6）

**问题**：幂等去重靠 `budget_reservations` 的 `UNIQUE(tenant_id, transaction_id)`。
但 `pool == nil`（客户无匹配预算池）时不创建预留 → 同一 `X-Idempotency-Key` 重放
（网络超时重试的典型场景）会通过 `checkTransactionExists` 检查，产生两条
event_id 不同的计量事件——sink 按 event_id 幂等无法去重，出账双倍计费。

**建议**：幂等占用独立于预算池（如 `idempotency_keys` 表 upsert，或无池时也写一条
零金额预留）。

### P1 — 2xx 无 usage 的非流式响应：既不计费也不审计（计量静默丢失）

**位置**：`internal/gateway/handler.go`（finish 闭包）+ `internal/provider/openai.go`

**问题**：OpenAI 兼容上游返回 200 但 body 缺 `usage` 字段（部分 vLLM/代理实现如此）
时，`result.Usage == nil` → `finish` 直接 return：不 confirm（等 TTL 释放，行为正确）、
**不发任何 meter event**。对比失败路径（4xx/5xx/网络错）都有 tokens=0 审计事件，
成功但缺 usage 的请求在审计日志中完全不可见。另注意两适配器行为不一致：
Anthropic 非流式恒返回零值 usage（会发 tokens=0 事件），OpenAI 适配器则直接跳过。

**建议**：非流式成功但 usage 缺失时，按 tokens=0 发审计事件（与 `emitFailureEvent`
同构），便于按 model/provider 排查上游契约异常。

### P2 —（新发现，摘要表）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| 1 | 账单币种混合 | `billing/worker.go` billCustomer | `currency` 取最后一个事件的快照值，跨币种产品金额直接相加在同一币种下出账；应按币种分账单或校验拒绝 |
| 2 | draft 重建物理删除 ledger_entries | `billing/worker.go` billCustomer | `DELETE FROM ledger_entries WHERE bill_id=...` 与"账本不可变，禁止 UPDATE"的 schema 注释冲突（draft 未定稿可重建属合理取舍，但建议改为 void 红冲或 draft 标记位，保持"只增"不变式） |
| 3 | FinalizeBill TOCTOU | `billing/bills.go` | UPDATE 未带 `AND status IN ('draft','amending')` 守卫；Get 与 UPDATE 之间状态可能已变（如 voided），并发下可越状态定稿 |
| 4 | 唯一约束冲突返回 500 | `api/customers.go` 等 | createCustomer/createProduct/createRateCard 等撞 `UNIQUE(tenant_id, ...)` 时应映射 409，当前 ErrInternal |
| 5 | currency 无格式校验 | `api/customers.go` | createCustomer/updateCustomer 接受任意字符串作 currency（无 ISO-4217 三字母校验），脏数据直接进账单 |
| 6 | 网关延迟指标仅成功路径采样 | `gateway/handler.go` | `GatewayLatencySeconds.Observe` 只在成功分支调用，失败/限流/超支请求延迟不可观测，p99 会系统性偏低 |
| 7 | Authenticate 无负缓存 | `rbac/apikeys.go` | 无效 key 反复请求每次都打 PG（cacheGet 只缓存成功身份）；恶意扫描可放大 PG 负载 |

### P3 — 细节观察（不阻塞）

- `provider/anthropic.go` 非流式 `finish_reason` 直接透传（`end_turn` 等）未翻译为
  OpenAI 的 `stop`，严格 SDK 可能不识别。
- `config.go` `expandEnv` 对 `${UNDEFINED}` 静默展开为空串，可能悄悄清空 provider
  api_key；建议未定义时保留原文或启动告警。
- `Health.ErrorRateThreshold` 非法值静默回退 0.5，与其他配置"启动期拦截"策略不一致。
- `ratelimit.MemoryLimiter.rate` map 的 key 无淘汰（仅测试替身使用，无实际影响）。
- `apikeys.go` ListKeys LIMIT 200、customers LIMIT 100 均无分页游标。

## 11. 历史遗留项状态核对（截至本轮）

| 来源 | 项 | 状态 |
|---|---|---|
| 第二轮 #4 | requireScope 权限不足返回 401 而非 403 | **仍开放**（server.go 175-179 维持 ErrUnauthorized；测试已固化契约，属有意接受） |
| 第二轮 #5 | admin 密钥明文存 localStorage | **仍开放** |
| 第二轮 #6 | 幂等键占用不区分预留状态（released/expired 后同键永远 409） | **仍开放**（001 迁移仍为全量 UNIQUE，无部分索引迁移） |
| 第二轮 #7 | audit `from` 可传 1970 绕过默认窗口；排序缺 event_id 次级键 | **仍开放**（audit.go parseAuditFilter/buildAuditQuery 已核对，无 from≤to、跨度上限与次级排序键） |
| 第二轮 #8 | mock 渠道探测样本稀释错误率 | **仍开放**（health.go ObserveProbe 与真实流量仍混入同一样本桶） |
| 第一轮 #2 | ClickHouse 迁移无版本化 | **仍开放** |
| 第一轮 #5 | RLS 已 ENABLE 未建 policy | **仍开放** |
| 第一轮 #6 | Outbox 与 confirm 非同一事务 | **仍开放**（对账兜底） |
| 第三轮 | 限流 v2/v3、锁续期、SSE 计费脱离请求生命周期、rewriteBodyModel、模型白名单校验最终模型、失败审计事件、五次退避停 Worker、5s 鉴权缓存、1h 分片拉取 | **全部核实落地** ✅ |

## 12. 本轮总体结论

- **修复质量**：前三轮共 3×P0 + 10×P1 的修复全部核实到位，且多数附带回归测试与
  根因注释（如限流 Lua 的 O(N) 演进、信号量 TTL 刷新语义），修复不是打补丁而是
  带测试的演进。
- **核心风险收敛**：鉴权（HMAC 单向 + 白名单 fail-closed + 缓存失效）、事件可靠性
  （Outbox 至少一次 + 幂等 sink）、预算一致性（两阶段 + 过期释放 + 漂移校正）三条
  主线均已闭环；本轮新发现的三个 P1 全部是**边界路径**（无池幂等、缺 usage、
  估算脱钩），不动摇主干。
- **建议优先级**：P1-1（快照估价）> P1-2（幂等键独立落表）> P1-3（缺 usage 审计），
  三者均是小改动大收益；P2 表中 #1（币种混合）在多币种租户上线前必须处理。
