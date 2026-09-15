-- DBA preflight：僅查詢，不修改資料。

SELECT
    name,
    engine,
    metadata_modification_time
FROM system.tables
WHERE database = 'icrown'
  AND name IN
  (
      'outcome_safety_net_pool_state',
      'outcome_safety_net_global_config',
      'outcome_safety_net_spin_ledger',
      'rtp_basic_pool_global_config',
      'rtp_basic_pool_state',
      'rtp_basic_pool_raw_event',
      'rtp_basic_pool_stats',
      'rtp_basic_pool_telemetry'
  )
ORDER BY name;

SELECT
    'outcome_safety_net_pool_state' AS table_name,
    count() AS row_count
FROM icrown.outcome_safety_net_pool_state
UNION ALL
SELECT
    'outcome_safety_net_global_config' AS table_name,
    count() AS row_count
FROM icrown.outcome_safety_net_global_config
UNION ALL
SELECT
    'outcome_safety_net_spin_ledger' AS table_name,
    count() AS row_count
FROM icrown.outcome_safety_net_spin_ledger
ORDER BY table_name;
