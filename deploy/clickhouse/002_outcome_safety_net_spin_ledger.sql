-- 安全網池（Phase 1）逐轉帳本草案。
--
-- 這是可回放、可去重的輸入帳本，不是 Controller 於本次 spin 決策前讀取的即時池狀態。
-- 同一 spin_id 重送時，寫入相同 key 的較新 updated_at；讀取最新版本時使用 FINAL。
-- 保留期限尚未決定，因此本版刻意不設定 TTL。

CREATE TABLE IF NOT EXISTS icrown.outcome_safety_net_spin_ledger_local
ON CLUSTER sg_cluster
(
    AGT_Agent1 String COMMENT 'Agent 1 GUID；帳本與池 key 的第一維',
    GM_GameCode LowCardinality(String) COMMENT '遊戲代碼；帳本與池 key 的第二維',
    GameServer_Version LowCardinality(String) COMMENT 'RTP 版本字串；保留前導零，例如 0970；帳本與池 key 的第三維',

    spin_id String COMMENT '來源 spin 的唯一識別；若來源為 GR_SEQ，寫入其十進位字串，作為冪等鍵',
    occurred_at DateTime64(6, 'Etc/GMT+4') COMMENT '最終結果結算時間；用於重放與時間範圍查詢',
    bet Decimal(38, 12) COMMENT '本轉最終押注',
    win Decimal(38, 12) COMMENT '本轉最終贏分；候選中未回傳的結果不得寫入',

    accounting_bucket LowCardinality(String) COMMENT 'NORMAL／SAFETY_NET／CUSTOMER_DEMAND／DEBT_REPAYMENT；只有 NORMAL 與 SAFETY_NET 納入安全網 AGG_DecayedBetSum、AGG_DecayedRtpDiff、AG_SpinCount',
    recorded_at DateTime64(6, 'Etc/GMT+4') COMMENT '帳本首次或重送寫入時間',
    updated_at DateTime64(6, 'Etc/GMT+4') COMMENT '帳本版本時間；ReplacingMergeTree 以此選擇最新版本'
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/icrown/outcome_safety_net_spin_ledger_local',
    '{replica}',
    updated_at
)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (AGT_Agent1, GM_GameCode, GameServer_Version, spin_id);

-- 同一 spin 必須落在同一 shard，否則重送版本可能無法在單一 shard 內正確去重。
CREATE TABLE IF NOT EXISTS icrown.outcome_safety_net_spin_ledger
ON CLUSTER sg_cluster
AS icrown.outcome_safety_net_spin_ledger_local
ENGINE = Distributed(
    'sg_cluster',
    'icrown',
    'outcome_safety_net_spin_ledger_local',
    cityHash64(AGT_Agent1, GM_GameCode, GameServer_Version, spin_id)
);
