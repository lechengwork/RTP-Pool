-- RTP基本池 v1 telemetry。由定時聚合服務寫入，不由 Kafka Engine 直接寫入。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_telemetry_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT '總代 ID',
    GM_GameCode LowCardinality(String) COMMENT '老虎機 ID',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本 ID；保留前導零，例如 0970',
    GR_SEQ UInt64 COMMENT '來源注單序號',
    event_at DateTime64(6, 'Etc/GMT+4') COMMENT 'spin 結算時間',
    bet Float64 COMMENT '當轉總下注',
    win Float64 COMMENT '當轉實際派彩',
    r Float64 COMMENT '當轉目標 RTP',
    is_buy_type UInt8 COMMENT '1＝購買類型',
    is_test_rng UInt8 COMMENT '必須為 0；測試 RNG 不得進入正式水池計算',
    metric Float64 COMMENT '決策前 Dl / Bl',
    band Float64 COMMENT '當轉動態容許帶寬',
    triggered UInt8 COMMENT 'Phase 1 必為 0；Phase 2 才可能為 1',
    candidate_count Nullable(UInt16) COMMENT '觸發時的 N；未觸發為 NULL',
    selected_candidate_win Nullable(Float64) COMMENT '觸發時選中的候選派彩；未觸發為 NULL',
    kafka_topic LowCardinality(String) COMMENT '原始 Kafka topic',
    kafka_partition UInt64 COMMENT '原始 Kafka partition',
    kafka_offset UInt64 COMMENT '原始 Kafka offset；管線去重鍵的一部分',
    aggregation_run_id UUID COMMENT '聚合批次 ID；供重播還原實際處理順序',
    processing_sequence UInt64 COMMENT '該批次內的處理順序',
    computed_at DateTime64(6, 'Etc/GMT+4') COMMENT '聚合服務計算時間'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_telemetry_local',
    '{replica}',
    computed_at
)
PARTITION BY toYYYYMMDD(event_at)
ORDER BY
(
    event_at,
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    kafka_topic,
    kafka_partition,
    kafka_offset
);

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_telemetry
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_telemetry_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_telemetry_local',
    cityHash64(kafka_topic, kafka_partition)
);
