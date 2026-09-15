-- RTP基本池累積統計。
-- 維度：Agent 1 GUID × 遊戲代碼 × RTP 版本。
-- 數值：bet、win。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_stats_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT 'Agent 1 GUID',
    GM_GameCode LowCardinality(String) COMMENT '遊戲代碼',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本字串；保留前導零，例如 0970',

    bet Decimal(38, 12) COMMENT '累積總押注',
    win Decimal(38, 12) COMMENT '累積總贏分'
)
ENGINE = ReplicatedSummingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_stats_local',
    '{replica}'
)
ORDER BY (AGT_Agent1, GM_GameCode, GameServer_Version);

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_stats
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_stats_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_stats_local',
    cityHash64(AGT_Agent1, GM_GameCode, GameServer_Version)
);
