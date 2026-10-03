-- 幂等键占用表（P1 修复：无预算池时幂等失效）。
-- 旧实现幂等去重依赖 budget_reservations 的 UNIQUE(tenant_id, transaction_id)，
-- 客户无预算池（不建预留）时同 X-Idempotency-Key 重放会产生两条计量事件（双倍计费）。
-- 独立占用表与预算池无关：请求路径 INSERT ON CONFLICT DO NOTHING 原子占用，
-- 冲突即拒绝（409 DUPLICATE_TRANSACTION）。
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    transaction_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, transaction_id)
);
-- 说明：不设保留期清理前的索引（主键已覆盖清理扫描：按 tenant_id 前缀删除）。
