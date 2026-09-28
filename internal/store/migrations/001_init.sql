-- 额度用 BIGINT，单位是 token。余额用 CHECK 兜底，应用层在任何路径下都不会把它减成负数。
-- quota_granted 只增不减（创建和充值），用来对账：
--   quota_granted = quota_balance + 已结算 charged + 在途 reserved

CREATE TABLE IF NOT EXISTS api_keys (
    id                BIGSERIAL PRIMARY KEY,
    key_hash          TEXT NOT NULL UNIQUE,
    key_prefix        TEXT NOT NULL,
    name              TEXT NOT NULL,
    quota_balance     BIGINT NOT NULL CHECK (quota_balance >= 0),
    quota_granted     BIGINT NOT NULL CHECK (quota_granted >= 0),
    rpm_limit         INTEGER NOT NULL DEFAULT 0,
    tpm_limit         INTEGER NOT NULL DEFAULT 0,
    concurrency_limit INTEGER NOT NULL DEFAULT 0,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 一个 channel 是一条上游连接：供应商种类、地址、优先级、权重。
-- models 用逗号分隔，"*" 表示接受任意模型名。
CREATE TABLE IF NOT EXISTS channels (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL,
    base_url   TEXT NOT NULL DEFAULT '',
    api_key    TEXT NOT NULL DEFAULT '',
    models     TEXT NOT NULL,
    priority   INTEGER NOT NULL DEFAULT 0,
    weight     INTEGER NOT NULL DEFAULT 1,
    timeout_ms INTEGER NOT NULL DEFAULT 30000,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 每一次用户请求对应一行预扣记录。request_id 主键保证同一请求不会预扣两次。
-- status: reserved 在途, settled 已结算, refunded 已全额退回。
CREATE TABLE IF NOT EXISTS reservations (
    request_id         TEXT PRIMARY KEY,
    api_key_id         BIGINT NOT NULL REFERENCES api_keys(id),
    model              TEXT NOT NULL DEFAULT '',
    channel_name       TEXT NOT NULL DEFAULT '',
    reserved           BIGINT NOT NULL CHECK (reserved >= 0),
    charged            BIGINT NOT NULL DEFAULT 0 CHECK (charged >= 0),
    unbilled           BIGINT NOT NULL DEFAULT 0 CHECK (unbilled >= 0),
    prompt_tokens      INTEGER NOT NULL DEFAULT 0,
    completion_tokens  INTEGER NOT NULL DEFAULT 0,
    status             TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (status IN ('reserved', 'settled', 'refunded'))
);

CREATE INDEX IF NOT EXISTS reservations_key_status_idx
    ON reservations (api_key_id, status);

CREATE INDEX IF NOT EXISTS reservations_status_created_idx
    ON reservations (status, created_at);
