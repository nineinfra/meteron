-- Meteron ClickHouse schema（详细设计文档 V1.0 第4章）
-- 明细表查询必须带 FINAL；报表优先使用 hourly 预聚合表

CREATE TABLE IF NOT EXISTS meter_events (
    tenant_id UUID,
    customer_id UUID,
    event_id String,
    event_version UInt64,
    transaction_id String,
    event_type String,
    event_time DateTime64(3),

    provider String,
    model String,
    task_type String,
    agent_id String,

    input_tokens UInt64,
    output_tokens UInt64,
    cached_input_tokens UInt64,

    status_code UInt16 DEFAULT 0,

    pricing_context_snapshot String,
    currency String,

    metadata Map(String, String),
    raw_payload String,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, customer_id, event_id, event_time);

CREATE TABLE IF NOT EXISTS meter_events_hourly (
    tenant_id UUID,
    customer_id UUID,
    task_type String,
    model String,
    provider String,
    agent_id String,
    hour DateTime,
    total_input_tokens UInt64,
    total_output_tokens UInt64,
    total_cached_input_tokens UInt64,
    request_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, customer_id, hour, task_type, model, provider);

-- 已有部署升级（幂等）：审计日志需按上游状态码过滤
ALTER TABLE meter_events ADD COLUMN IF NOT EXISTS status_code UInt16 DEFAULT 0;

-- 数据保留策略（P1）：建表时未设置 TTL，明细表会无界增长。
-- 单事件最多携带 64KB raw_payload（完整请求体，见 gateway/handler.go），
-- 长期留存既是存储成本问题，也是隐私风险（prompt 正文落库）。
-- 13 个月保留覆盖年度对账与审计回溯；如合规要求更短，调小此值即可（按分区整块删除）。
ALTER TABLE meter_events MODIFY TTL toDateTime(event_time) + INTERVAL 13 MONTH;

-- 预聚合表为小时粒度汇总（体积小），保留 3 年供长期趋势与容量分析。
ALTER TABLE meter_events_hourly MODIFY TTL toDateTime(hour) + INTERVAL 3 YEAR;
