-- RTP基本池 v1 telemetry / 事件 log。
-- 每一轉 append 一列；這是調參、稽核與 Shadow Mode 驗收的原始證據。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_event_log_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT '總代 ID',
    GM_GameCode LowCardinality(String) COMMENT '老虎機 ID',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本 ID；保留前導零，例如 0970',

    event_at DateTime64(6, 'Etc/GMT+4') COMMENT '當轉結算與記錄時間',
    bet Float64 COMMENT '當轉總下注',
    win Float64 COMMENT '當轉實際派彩',
    r Float64 COMMENT '當轉目標 RTP',
    is_buy_type UInt8 COMMENT '1＝購買類型；只記帳、不參與候選挑選',
    metric Float64 COMMENT '決策前偏差：Dl / Bl',
    band Float64 COMMENT '當轉動態容許帶寬',
    triggered UInt8 COMMENT '1＝當轉走候選挑選；0＝單一公平抽樣',
    candidate_count Nullable(UInt16) COMMENT '觸發時的 N；未觸發為 NULL',
    selected_candidate_win Nullable(Float64) COMMENT '觸發時選中的候選派彩；未觸發為 NULL'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_event_log_local',
    '{replica}'
)
PARTITION BY toYYYYMM(event_at)
ORDER BY (event_at, AGT_Agent1, GM_GameCode, GameServer_Version);

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_event_log
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_event_log_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_event_log_local',
    cityHash64(AGT_Agent1, GM_GameCode, GameServer_Version)
);
