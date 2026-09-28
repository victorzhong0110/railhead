-- 同一把密钥的余额拆到多行上。热路径只锁其中一行，不再把所有请求堵在 api_keys 这一行。
-- 对账时用 SUM(quota_shards.balance)，不读 api_keys.quota_balance（那一列只在创建和充值时更新，会过期）。
-- 不变量：quota_granted = SUM(shard.balance) + Σ settled.charged + Σ reserved.reserved

CREATE TABLE IF NOT EXISTS quota_shards (
    key_id  BIGINT NOT NULL REFERENCES api_keys(id),
    shard   SMALLINT NOT NULL CHECK (shard >= 0),
    balance BIGINT NOT NULL CHECK (balance >= 0),
    PRIMARY KEY (key_id, shard)
);

ALTER TABLE reservations ADD COLUMN IF NOT EXISTS shard SMALLINT NOT NULL DEFAULT 0;

-- 已有密钥的余额原样放到 0 号分片。新密钥由应用按配置的分片数摊开。
INSERT INTO quota_shards (key_id, shard, balance)
SELECT id, 0, quota_balance FROM api_keys
ON CONFLICT (key_id, shard) DO NOTHING;
