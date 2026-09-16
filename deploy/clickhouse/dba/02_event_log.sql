-- RTP基本池 MVP 事件表。此階段只建儲存結構，不建立 Kafka Engine 或 MV。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_event_log_local
ON CLUSTER sg_cluster
(
    GR_SEQ UInt64 COMMENT '來源注單序號',
    AGT_Agent1 LowCardinality(String) COMMENT '總代 ID',
    GM_GameCode LowCardinality(String) COMMENT '老虎機 ID',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本 ID',
    GR_EndTime DateTime64(9, 'Etc/GMT+4') COMMENT 'spin 結算時間',
    GR_Bets Decimal(22, 4) COMMENT '當轉總下注',
    GR_Win Decimal(22, 4) COMMENT '當轉總派彩',
    GR_BetType UInt8 COMMENT '0=一般、1=FeatureBuy、2=ExtraBet、3=SuperFeatureBuy',
    IsTestRng Bool COMMENT '測試 RNG 標記',

    target_rtp Nullable(Float64) COMMENT '該 RTP 版本的目標返還率；格式非法為 NULL',
    is_buy_type UInt8 COMMENT 'GR_BetType 為 1 或 3 時等於 1',
    metric Nullable(Float64) COMMENT '服務計算前的 Dl / Bl；尚未處理為 NULL',
    band Nullable(Float64) COMMENT '當轉動態帶寬；尚未處理為 NULL',
    triggered Nullable(UInt8) COMMENT '服務處理後 Phase 1 寫 0；尚未處理為 NULL',
    candidate_count Nullable(UInt16) COMMENT '觸發時 N；Phase 1 為 NULL',
    selected_candidate_win Nullable(Float64) COMMENT '觸發時選中派彩；Phase 1 為 NULL',
    processed_at Nullable(DateTime64(6, 'Etc/GMT+4')) COMMENT '服務完成本事件計算的時間',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '事件版本時間'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_event_log_local',
    '{replica}',
    updated_at
)
PARTITION BY toYYYYMMDD(GR_EndTime)
ORDER BY (AGT_Agent1, GM_GameCode, GameServer_Version, GR_SEQ);

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_event_log
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_event_log_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_event_log_local',
    cityHash64(AGT_Agent1, GM_GameCode, GameServer_Version)
);

CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_event_log_latest
ON CLUSTER sg_cluster
AS
SELECT
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    GR_SEQ,
    GR_EndTime,
    GR_Bets,
    GR_Win,
    GR_BetType,
    IsTestRng,
    target_rtp,
    is_buy_type,
    metric,
    band,
    triggered,
    candidate_count,
    selected_candidate_win,
    processed_at,
    updated_at
FROM icrown.rtp_basic_pool_event_log
FINAL;

-- 主管要求的五欄統計；MVP 使用 View，避免額外表與同步邏輯。
CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_stats
ON CLUSTER sg_cluster
AS
SELECT
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    sum(CAST(GR_Bets AS Decimal(38, 12))) AS bet,
    sum(CAST(GR_Win AS Decimal(38, 12))) AS win
FROM icrown.rtp_basic_pool_event_log_latest
WHERE IsTestRng = 0
GROUP BY
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version;
