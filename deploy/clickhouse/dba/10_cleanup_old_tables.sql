-- 破壞性操作：只有 01_preflight 顯示舊表全為 0，且 09_postflight 全部通過後才能執行。

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
