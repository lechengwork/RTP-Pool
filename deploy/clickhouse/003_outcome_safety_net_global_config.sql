-- 安全網池（Phase 1）全域參數草案。
--
-- 全域參數不重複寫入每個池；池表只保存 epsilon_override。
-- 本檔只建結構，不預先 INSERT 任何數值。λ、ε、暖身門檻、K 必須先經歷史回放校準後才設定。

CREATE TABLE IF NOT EXISTS icrown.outcome_safety_net_global_config_local
ON CLUSTER sg_cluster
(
    config_id LowCardinality(String) COMMENT '設定識別；正式啟用時使用單一 default 值',

    lambda Decimal(18, 12) COMMENT 'EWMA 衰減係數 λ；有效記憶長度約為 1 / (1 − λ) 轉',
    epsilon Decimal(9, 6) COMMENT '全域粗偏移門檻 ε，例如 0.100000 = 10%',
    warmup_spin_count UInt64 COMMENT '暖身門檻；spin_count 未達此值時安全網只記帳、不介入',
    candidate_count UInt16 COMMENT 'K；安全網介入時要求的真實候選結果數',

    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '設定版本時間；ReplacingMergeTree 以此選擇最新版本',
    updated_by String COMMENT '設定異動者或變更來源'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/outcome_safety_net_global_config_local',
    '{replica}',
    updated_at
)
ORDER BY config_id;

CREATE TABLE IF NOT EXISTS icrown.outcome_safety_net_global_config
ON CLUSTER sg_cluster
AS icrown.outcome_safety_net_global_config_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'outcome_safety_net_global_config_local',
    cityHash64(config_id)
);
