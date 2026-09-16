-- 最後一步；執行前須由 01_preflight 確認三張舊 local 表仍為 0 筆，且 04_verify 已通過。

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
