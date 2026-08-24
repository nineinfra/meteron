-- API-Key 模型增强（参考 new-api/sub2api）：
-- ip_whitelist：IP 白名单（空数组=不限制；支持单 IP 与 CIDR）
-- model_allow_list：模型白名单（空数组=允许全部模型）
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS ip_whitelist TEXT[] DEFAULT '{}';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS model_allow_list TEXT[] DEFAULT '{}';
