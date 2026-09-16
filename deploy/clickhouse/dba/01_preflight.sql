-- 舊三張 local 表應存在且 total_rows = 0；新物件在首次執行前應不存在。
SELECT
    name,
    engine,
    total_rows,
    metadata_modification_time
FROM system.tables
WHERE database = 'icrown'
  AND
  (
      name LIKE 'outcome_safety_net%'
      OR name LIKE 'rtp_basic_pool%'
  )
ORDER BY name;
