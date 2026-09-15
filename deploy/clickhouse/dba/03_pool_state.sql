-- RTP基本池狀態。數學狀態為 Dl、Bl、W、override_mode；last_seen 只服務 30 天 TTL。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_state_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT '總代 ID',
    GM_GameCode LowCardinality(String) COMMENT '老虎機 ID',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本 ID；保留前導零，例如 0970',
    Dl Float64 DEFAULT 0.0 COMMENT 'Dl ← λ × Dl + (r × bet − win)',
    Bl Float64 DEFAULT 0.0 COMMENT 'Bl ← λ × Bl + bet',
    W Float64 DEFAULT 0.0 COMMENT 'W ← λ × W + 1',
    override_mode Enum8('AUTO' = 1, 'FORCE_ON' = 2, 'FORCE_OFF' = 3) DEFAULT 'AUTO' COMMENT '每池控制覆寫',
    last_seen DateTime64(6, 'Etc/GMT+4') COMMENT '最後一筆 spin 時間；TTL 依據',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '狀態版本時間'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_state_local',
    '{replica}',
    updated_at
)
ORDER BY (AGT_Agent1, GM_GameCode, GameServer_Version)
TTL last_seen + INTERVAL 30 DAY DELETE;

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_state
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_state_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_state_local',
    cityHash64(AGT_Agent1, GM_GameCode, GameServer_Version)
);

CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_state_latest
ON CLUSTER sg_cluster
AS
SELECT
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    Dl,
    Bl,
    W,
    override_mode,
    last_seen,
    updated_at
FROM icrown.rtp_basic_pool_state
FINAL;
