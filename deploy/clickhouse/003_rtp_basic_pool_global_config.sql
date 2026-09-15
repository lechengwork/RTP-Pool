-- RTP基本池全域常數。
-- λ、ε、VOL 為校準三元組；不得單獨調整。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_global_config_local
ON CLUSTER sg_cluster
(
    config_id LowCardinality(String) COMMENT '設定識別；正式啟用時使用單一 default 值',

    lambda Float64 COMMENT 'λ；目前規格值 0.9999975',
    epsilon Float64 COMMENT 'ε；目前規格值 0.05',
    band_sigma_k Float64 COMMENT 'K；動態帶寬的 σ 倍數，目前規格值 3',
    vol Float64 COMMENT 'VOL；全平台波動係數，目前規格值 15',
    w_min Float64 COMMENT 'W_MIN；啟動門檻，目前規格值 10000',
    candidate_count UInt16 COMMENT 'N；候選數，目前規格值 10，待上線觀察調整',

    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '設定版本時間',
    updated_by String COMMENT '設定異動者或變更來源'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/rtp_basic_pool_global_config_local',
    '{replica}',
    updated_at
)
ORDER BY config_id;

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_global_config
ON CLUSTER sg_cluster
AS icrown.rtp_basic_pool_global_config_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'rtp_basic_pool_global_config_local',
    cityHash64(config_id)
);
