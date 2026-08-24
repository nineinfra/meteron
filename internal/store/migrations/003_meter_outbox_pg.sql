-- Meter Outbox：计量事件投递 outbox（P1 修复：崩溃不丢事件）
-- 事件先持久化到 PG，再投递 Kafka；投递失败由 relay 周期重试，
-- 已发布记录保留 7 天用于审计，之后由 relay 清理。
CREATE TABLE IF NOT EXISTS meter_outbox (
    event_id UUID PRIMARY KEY,
    kafka_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP
);
-- 部分索引：relay 只扫描未发布事件
CREATE INDEX IF NOT EXISTS idx_meter_outbox_pending ON meter_outbox(created_at) WHERE published_at IS NULL;
