-- bills 周期唯一性约束（P1）：防止并发/重跑产生同周期重复账单。
--
-- 背景：RunBilling 依赖 Redis 分布式锁串行化跑批。锁 TTL 过期（长跑批无续期）、
-- 多实例时钟漂移或 Redis 故障切换时，两个实例可能同时为同一
-- (tenant_id, customer_id, period_start, period_end) 生成 draft 账单。
-- "删除旧 draft 再重建"在同一实例内是幂等的，但并发事务互相看不到对方的
-- draft，会各插一张；后续定稿即对同一周期出现两张有效账单（重复计费）。
-- 修复分两部分：billing.Worker 增加锁续期（TTL 内周期续租）+ 本索引兜底。
--
-- 采用部分唯一索引：仅约束 draft/finalized（当前有效账单），
-- V1 的 amending 修正账单可与已定稿账单共存，不受此约束。
--
-- 注意：本文件由 splitSQL 按分号切分执行（不解析注释），
-- 因此注释内不得出现分号。若需补充示例 SQL，请改写为无分号表述。
CREATE UNIQUE INDEX IF NOT EXISTS uq_bills_period_active
    ON bills (tenant_id, customer_id, period_start, period_end)
    WHERE status IN ('draft', 'finalized')
