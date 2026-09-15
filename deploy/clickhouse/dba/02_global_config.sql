-- RTP基本池全域常數。λ、ε、VOL 是校準三元組，不得單獨調整。

CREATE TABLE IF NOT EXISTS icrown.rtp_basic_pool_global_config_local
ON CLUSTER sg_cluster
(
    config_id LowCardinality(String) COMMENT '設定識別；正式使用 default',
    lambda Float64 COMMENT 'λ；規格初值 0.9999975',
    epsilon Float64 COMMENT 'ε；規格初值 0.05',
    band_sigma_k Float64 COMMENT 'K；動態帶寬 σ 倍數，規格初值 3',
    vol Float64 COMMENT 'VOL；規格初值 15',
    w_min Float64 COMMENT 'W_MIN；規格初值 10000',
    candidate_count UInt16 COMMENT 'N；候選數，規格初值 10',
    global_override_mode Enum8('AUTO' = 1, 'FORCE_OFF' = 2) DEFAULT 'FORCE_OFF' COMMENT '全域 kill switch；Phase 1 固定 FORCE_OFF，Phase 2 經核准後才可改 AUTO',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '設定版本時間',
    updated_by String COMMENT '異動者或來源'
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

CREATE VIEW IF NOT EXISTS icrown.rtp_basic_pool_global_config_latest
ON CLUSTER sg_cluster
AS
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
FROM
(
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
    FROM icrown.rtp_basic_pool_global_config
    ORDER BY config_id, updated_at DESC
    LIMIT 1 BY config_id
);

INSERT INTO icrown.rtp_basic_pool_global_config
(
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
)
SELECT
    'default' AS config_id,
    toFloat64(0.9999975) AS lambda,
    toFloat64(0.05) AS epsilon,
    toFloat64(3) AS band_sigma_k,
    toFloat64(15) AS vol,
    toFloat64(10000) AS w_min,
    toUInt16(10) AS candidate_count,
    CAST('FORCE_OFF' AS Enum8('AUTO' = 1, 'FORCE_OFF' = 2)) AS global_override_mode,
    now64(6, 'Etc/GMT+4') AS updated_at,
    'initial-spec-2026-09-15' AS updated_by
WHERE NOT EXISTS
(
    SELECT
        config_id
    FROM icrown.rtp_basic_pool_global_config_latest
    WHERE config_id = 'default'
);
