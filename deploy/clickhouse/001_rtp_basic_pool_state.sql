-- RTP基本池狀態（Phase 1）。
-- 數學狀態只有 Dl、Bl、W、override_mode；last_seen 為 30 天 TTL 的營運 metadata。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_state_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT '總代 ID；水池 key 的第一維',
    GM_GameCode LowCardinality(String) COMMENT '老虎機 ID；水池 key 的第二維',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本 ID；保留前導零，例如 0970；水池 key 的第三維',

    Dl Float64 COMMENT 'EWMA 水位：Dl ← λ × Dl + (r × bet − win)',
    Bl Float64 COMMENT 'EWMA 下注：Bl ← λ × Bl + bet',
    W Float64 COMMENT 'EWMA 轉數：W ← λ × W + 1',
    override_mode Enum8('AUTO' = 1, 'FORCE_ON' = 2, 'FORCE_OFF' = 3) COMMENT '控制覆寫；預設 AUTO',

    last_seen DateTime64(6, 'Etc/GMT+4') COMMENT '最後一筆 spin 時間；僅用於 30 天 TTL',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '狀態版本時間；ReplacingMergeTree 以此選擇最新版本'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_state_local',
    '{replica}',
    updated_at
)
PARTITION BY toYYYYMM(last_seen)
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
