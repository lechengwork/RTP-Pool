-- RTP基本池直接訂閱 SG slot game Kafka topic。
-- Schema 與 SG 線上 icrown.ke_slotgame 對齊；consumer group 必須獨立，禁止使用既有 clickhouse group。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_raw_event_local
ON CLUSTER sg_cluster
(
    GR_SEQ UInt64 COMMENT '來源注單序號；供回放與來源對帳',
    AGT_Agent1 LowCardinality(String) COMMENT '總代 ID',
    GM_GameCode LowCardinality(String) COMMENT '老虎機 ID',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本 ID；保留來源字串',
    GR_EndTime DateTime64(9, 'Etc/GMT+4') COMMENT 'spin 結算時間',
    GR_Bets Decimal(22, 4) COMMENT '當轉總下注',
    GR_Win Decimal(22, 4) COMMENT '當轉總派彩',
    GR_BetType UInt8 COMMENT '下注類型：0=一般、1=FeatureBuy、2=ExtraBet、3=SuperFeatureBuy；isBuyType = 1 或 3',
    IsTestRng Bool COMMENT '來源是否為測試 RNG；原始表保留，聚合時不得計入正式水池',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic',
    kafka_partition UInt64 COMMENT 'Kafka partition',
    kafka_offset UInt64 COMMENT 'Kafka offset；管線去重鍵的一部分',
    ingested_at DateTime64(6, 'Etc/GMT+4') COMMENT 'ClickHouse 落地時間'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_raw_event_local',
    '{replica}',
    ingested_at
)
PARTITION BY toYYYYMMDD(GR_EndTime)
ORDER BY (kafka_topic, kafka_partition, kafka_offset);

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_raw_event
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_raw_event_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_raw_event_local',
    cityHash64(kafka_topic, kafka_partition)
);

-- Kafka commit 與 ClickHouse 寫入非原子；跨 shard 重送時由此 View 依來源 offset 去重。
-- 聚合服務只讀本 View，不直接讀 raw_event。
CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_raw_event_deduplicated
ON CLUSTER sg_cluster
AS
SELECT
    argMax(GR_SEQ, ingested_at) AS GR_SEQ,
    argMax(AGT_Agent1, ingested_at) AS AGT_Agent1,
    argMax(GM_GameCode, ingested_at) AS GM_GameCode,
    argMax(GameServer_Version, ingested_at) AS GameServer_Version,
    argMax(GR_EndTime, ingested_at) AS GR_EndTime,
    argMax(GR_Bets, ingested_at) AS GR_Bets,
    argMax(GR_Win, ingested_at) AS GR_Win,
    argMax(GR_BetType, ingested_at) AS GR_BetType,
    argMax(IsTestRng, ingested_at) AS IsTestRng,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    max(ingested_at) AS ingested_at
FROM icrown.rtp_basic_pool_raw_event
GROUP BY
    kafka_topic,
    kafka_partition,
    kafka_offset;

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_kafka_source
ON CLUSTER sg_cluster
(
    GR_SEQ UInt64,
    GM_GameCode LowCardinality(String),
    AGT_Agent1 LowCardinality(String),
    AGT_Agent2 LowCardinality(String),
    AGT_Agent3 LowCardinality(String),
    PLY_GUID String,
    AGT_AccountID LowCardinality(String),
    PLY_AccountID String,
    GR_Currency LowCardinality(String),
    GR_Bets Decimal(22, 4),
    GR_GamebleBets Decimal(22, 4),
    GR_Win Decimal(22, 4),
    GR_ValidBets Decimal(22, 4),
    GR_NetWin Decimal(22, 4),
    GR_Jackpot Decimal(22, 4),
    GR_JackpotType UInt8,
    GR_JackpotContribute Decimal(22, 4),
    GR_Record String,
    GR_FlagFreeGame Bool,
    GR_FlagGamble Bool,
    GR_FlagInterrupt Bool,
    GR_StartTime DateTime64(9, 'Etc/GMT+4'),
    GR_EndTime DateTime64(9, 'Etc/GMT+4'),
    GR_IPAddress String,
    GameServer_Version LowCardinality(String),
    GameServer_Name LowCardinality(String),
    GR_BeforeBalance Decimal(22, 4),
    GR_AfterBalance Decimal(22, 4),
    GR_CreateDateTime DateTime64(9, 'Etc/GMT+4'),
    GR_ClientType UInt8,
    GR_BetType UInt8,
    GR_RecordCompress String,
    Multiplier Decimal(10, 4),
    GitVersion LowCardinality(String),
    GitCommitHash FixedString(40),
    MathBet Decimal(18, 4),
    MathTotalBet Decimal(22, 4),
    IsTestRng Bool,
    Extend Array(Int64),
    UsedRng Array(Int64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '172.31.15.82:9092,172.31.12.252:9092,172.31.5.49:9092',
    kafka_topic_list = 'LCGAME.slotGame.winloseV1',
    kafka_group_name = 'rtp_basic_pool_sg_v1',
    kafka_format = 'JSONEachRow',
    date_time_input_format = 'best_effort',
    kafka_num_consumers = 8;

-- 與現行 RTP Monitor 的 Kafka/MV 模式一致：每個 consumer 寫入同節點的 replicated local target。
CREATE MATERIALIZED VIEW IF NOT EXISTS icrown.rtp_basic_pool_kafka_to_raw_event
ON CLUSTER sg_cluster
TO icrown.rtp_basic_pool_raw_event_local
AS
SELECT
    GR_SEQ,
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    GR_EndTime,
    GR_Bets,
    GR_Win,
    GR_BetType,
    IsTestRng,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(6, 'Etc/GMT+4') AS ingested_at
FROM icrown.rtp_basic_pool_kafka_source;
