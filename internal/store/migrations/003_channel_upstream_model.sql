-- 客户端模型名和上游真实模型可以不同，故障转移时才能从 MiniMax-M3 换到 MiniMax-M2.5。
ALTER TABLE channels ADD COLUMN IF NOT EXISTS upstream_model TEXT NOT NULL DEFAULT '';
