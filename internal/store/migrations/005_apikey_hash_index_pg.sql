-- API-Key 鉴权热路径索引：
-- Authenticate 按 key_hash 等值查找（每个网关/管理请求执行一次），
-- 无索引时全表扫描，鉴权延迟随密钥数线性劣化（亦可被无密钥高频请求放大为 DoS 向量）。
-- 唯一约束同时兜底 Bootstrap 并发首启重复插入引导密钥的竞态。
CREATE UNIQUE INDEX IF NOT EXISTS idx_apikey_hash ON api_keys (key_hash);
