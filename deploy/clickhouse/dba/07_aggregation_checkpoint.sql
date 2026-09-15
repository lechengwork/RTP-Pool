-- 定時聚合服務 checkpoint。每個 topic/partition 一列最新成功處理位置。
-- 服務必須先完成 state/stats/telemetry 寫入，再推進 checkpoint；失敗時不得前移。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_aggregation_checkpoint_local
ON CLUSTER sg_cluster
(
    consumer_name LowCardinality(String) COMMENT '聚合服務或作業識別',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic',
    kafka_partition UInt64 COMMENT 'Kafka partition',
    last_processed_offset UInt64 COMMENT '已完整寫入所有下游表的最後 offset',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT 'checkpoint 版本時間'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_aggregation_checkpoint_local',
    '{replica}',
    updated_at
)
ORDER BY (consumer_name, kafka_topic, kafka_partition);

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_aggregation_checkpoint
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_aggregation_checkpoint_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_aggregation_checkpoint_local',
    cityHash64(consumer_name, kafka_topic, kafka_partition)
);

CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_aggregation_checkpoint_latest
ON CLUSTER sg_cluster
AS
SELECT
    consumer_name,
    kafka_topic,
    kafka_partition,
    last_processed_offset,
    updated_at
FROM
(
    SELECT
        consumer_name,
        kafka_topic,
        kafka_partition,
        last_processed_offset,
        updated_at
    FROM icrown.rtp_basic_pool_aggregation_checkpoint
    ORDER BY consumer_name, kafka_topic, kafka_partition, updated_at DESC
    LIMIT 1 BY consumer_name, kafka_topic, kafka_partition
);
