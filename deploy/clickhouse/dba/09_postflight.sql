-- 新物件建立後、刪除舊物件前的驗收。DBA 需具 READ ON REMOTE。

SELECT
    hostName() AS host_name,
    name,
    engine,
    total_rows
FROM clusterAllReplicas('sg_cluster', system.tables)
WHERE database = 'icrown'
  AND name IN
  (
      'rtp_basic_pool_global_config_local',
      'rtp_basic_pool_global_config',
      'rtp_basic_pool_global_config_latest',
      'rtp_basic_pool_state_local',
      'rtp_basic_pool_state',
      'rtp_basic_pool_state_latest',
      'rtp_basic_pool_raw_event_local',
      'rtp_basic_pool_raw_event',
      'rtp_basic_pool_raw_event_deduplicated',
      'rtp_basic_pool_kafka_source',
      'rtp_basic_pool_kafka_to_raw_event',
      'rtp_basic_pool_stats_local',
      'rtp_basic_pool_stats',
      'rtp_basic_pool_stats_totals',
      'rtp_basic_pool_telemetry_local',
      'rtp_basic_pool_telemetry',
      'rtp_basic_pool_aggregation_checkpoint_local',
      'rtp_basic_pool_aggregation_checkpoint',
      'rtp_basic_pool_aggregation_checkpoint_latest'
  )
ORDER BY host_name, name;

SELECT
    config_id,
    lambda,
    epsilon,
    band_sigma_k,
    vol,
    w_min,
    candidate_count,
    global_override_mode,
    updated_at,
    updated_by
FROM icrown.rtp_basic_pool_global_config_latest
WHERE config_id = 'default';

SELECT
    hostName() AS host_name,
    database,
    table,
    consumer_id,
    assignments.topic,
    assignments.partition_id,
    assignments.current_offset,
    last_poll_time,
    num_messages_read,
    last_commit_time,
    num_commits,
    is_currently_used,
    missing_dependencies
FROM clusterAllReplicas('sg_cluster', system.kafka_consumers)
WHERE database = 'icrown'
  AND table = 'rtp_basic_pool_kafka_source'
ORDER BY host_name, consumer_id;

SELECT
    count() AS raw_event_rows,
    min(GR_EndTime) AS earliest_event_time,
    max(GR_EndTime) AS latest_event_time,
    countDistinct(tuple(kafka_topic, kafka_partition, kafka_offset)) AS distinct_source_offsets
FROM icrown.rtp_basic_pool_raw_event
FINAL;

SELECT
    count() AS deduplicated_event_rows,
    min(GR_EndTime) AS earliest_event_time,
    max(GR_EndTime) AS latest_event_time
FROM icrown.rtp_basic_pool_raw_event_deduplicated;
