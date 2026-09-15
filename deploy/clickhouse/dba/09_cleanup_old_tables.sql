-- 破壞性操作：只有 01_preflight.sql 顯示三張舊表皆為 0 筆，且新表驗證成功後才能執行。

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
