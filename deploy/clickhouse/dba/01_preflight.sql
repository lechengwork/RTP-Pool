-- DBA preflight：僅查詢，不修改資料。

SELECT
    hostName() AS host_name,
    name,
    engine,
    total_rows,
    metadata_modification_time
FROM clusterAllReplicas('sg_cluster', system.tables)
WHERE database = 'icrown'
  AND name IN
  (
      'outcome_safety_net_pool_state',
      'outcome_safety_net_global_config',
      'outcome_safety_net_spin_ledger',
      'rtp_basic_pool_global_config',
      'rtp_basic_pool_state',
      'rtp_basic_pool_kafka_source',
      'rtp_basic_pool_raw_event',
      'rtp_basic_pool_raw_event_deduplicated',
      'rtp_basic_pool_stats',
      'rtp_basic_pool_telemetry',
      'rtp_basic_pool_aggregation_checkpoint'
  )
ORDER BY name;

-- 六個舊物件應存在；三個 _local 表在每個 replica 的 total_rows 都必須為 0。
SELECT
    hostName() AS host_name,
    name,
    engine,
    total_rows
FROM clusterAllReplicas('sg_cluster', system.tables)
WHERE database = 'icrown'
  AND name IN
  (
      'outcome_safety_net_pool_state_local',
      'outcome_safety_net_global_config_local',
      'outcome_safety_net_spin_ledger_local'
  )
ORDER BY host_name, name;

-- 現有 Kafka source 應持續消費且沒有 missing dependency。
SELECT
    hostName() AS host_name,
    database,
    table,
    consumer_id,
    assignments.topic,
    assignments.partition_id,
    last_poll_time,
    last_commit_time,
    is_currently_used,
    missing_dependencies
FROM clusterAllReplicas('sg_cluster', system.kafka_consumers)
WHERE database = 'icrown'
  AND table = 'ke_slotgame'
ORDER BY host_name, consumer_id;
