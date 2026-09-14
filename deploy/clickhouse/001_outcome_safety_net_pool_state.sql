-- 安全網池（Phase 1）狀態表草案。
--
-- 一個邏輯池 = AGT_Agent1 × GM_GameCode × GameServer_Version。
-- r 不落表，執行期以 GameServer_Version 轉為目標 RTP（例如 0970 -> 0.970）。
-- λ、全域 ε、暖身門檻與 K 是全域設定；每池只允許 epsilon_override 覆寫。
--
-- 寫入模型：每次狀態變更 INSERT 一個新版本，讀取最新狀態時使用 FINAL。
-- 同一個池的 spin 必須由應用層序列化；ReplacingMergeTree 不提供 read-modify-write 的原子性。

CREATE TABLE IF NOT EXISTS icrown.outcome_safety_net_pool_state_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT 'Agent 1 GUID；安全網池 key 的第一維',
    GM_GameCode LowCardinality(String) COMMENT '遊戲代碼；安全網池 key 的第二維',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本字串；保留前導零，例如 0970；安全網池 key 的第三維',

    B Decimal(38, 12) COMMENT '衰減後累積押注：B ← λ × B + bet',
    D Decimal(38, 12) COMMENT '衰減後 RTP 差額：D ← λ × D + (bet × r − win)',
    spin_count UInt64 COMMENT '累積納入安全網的轉數；暖身判斷使用，不計客戶需求或記債池還款轉',
    last_seen DateTime64(6, 'Etc/GMT+4') COMMENT '最後一筆納入安全網的 spin 時間；TTL 以此欄位計算',

    epsilon_override Nullable(Decimal(9, 6)) COMMENT '個別池的粗偏移門檻覆寫；NULL 時使用全域 ε，例如 0.100000 = 10%',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '狀態版本時間；ReplacingMergeTree 以此選擇最新版本'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/outcome_safety_net_pool_state_local',
    '{replica}',
    updated_at
)
PARTITION BY toYYYYMM(last_seen)
ORDER BY (AGT_Agent1, GM_GameCode, GameServer_Version)
TTL last_seen + INTERVAL 60 DAY DELETE;

-- 同一個池一律落到同一 shard，才能讓 FINAL 與 ReplacingMergeTree 正確收斂到一列。
CREATE TABLE IF NOT EXISTS icrown.outcome_safety_net_pool_state
ON CLUSTER sg_cluster
AS icrown.outcome_safety_net_pool_state_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'outcome_safety_net_pool_state_local',
    cityHash64(AGT_Agent1, GM_GameCode, GameServer_Version)
);
