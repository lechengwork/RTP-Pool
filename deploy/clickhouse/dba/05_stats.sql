-- 主管要求的純累積統計表；聚合服務只能寫「尚未處理事件的增量 bet/win」，不可重寫累計快照。

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

-- SummingMergeTree 的背景合併不是同步完成；查詢端一律讀這個 View，不直接假設基表已合併。
CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_stats_totals
ON CLUSTER sg_cluster
AS
SELECT
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    sum(bet) AS bet,
    sum(win) AS win
FROM icrown.rtp_basic_pool_stats
GROUP BY
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version;
