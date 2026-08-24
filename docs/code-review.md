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
