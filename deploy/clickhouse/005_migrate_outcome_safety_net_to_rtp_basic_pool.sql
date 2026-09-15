-- 遷移舊 outcome_safety_net 表至 RTP基本池命名。
-- 執行前置條件：三個 preflight row_count 都必須是 0。
-- 本檔會 DROP 舊表；若任一表已有資料，不可執行，必須另做資料保留遷移。

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

-- 僅在三列皆為 0 時繼續。先執行 001 至 004 建立新表，再執行下列 DROP。
DROP TABLE IF EXISTS icrown.outcome_safety_net_spin_ledger
ON CLUSTER sg_cluster SYNC;

DROP TABLE IF EXISTS icrown.outcome_safety_net_spin_ledger_local
ON CLUSTER sg_cluster SYNC;

DROP TABLE IF EXISTS icrown.outcome_safety_net_global_config
ON CLUSTER sg_cluster SYNC;

DROP TABLE IF EXISTS icrown.outcome_safety_net_global_config_local
ON CLUSTER sg_cluster SYNC;

DROP TABLE IF EXISTS icrown.outcome_safety_net_pool_state
ON CLUSTER sg_cluster SYNC;

DROP TABLE IF EXISTS icrown.outcome_safety_net_pool_state_local
ON CLUSTER sg_cluster SYNC;
