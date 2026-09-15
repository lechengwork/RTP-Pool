# RTP基本池 ClickHouse 遷移包（DBA 審閱版）

**狀態：僅供審閱，尚未執行。**本包依 `規格文件/02_RTP基本池控制_Server實作規格.md` 的 Phase 1 Shadow Mode 設計。

驗證範圍、實證與尚未解除的上線阻擋見 `VALIDATION.md`。在該文件的阻擋全部關閉前，不得交付執行。

## 目的與資料流

```text
Server → Kafka → ClickHouse Kafka Engine → 原始事件表
                                           ↓
                                  定時聚合服務
                                  ├─ RTP基本池狀態
                                  ├─ 累積統計
                                  └─ telemetry 事件 log
```

Phase 1 一律單一公平抽樣，不做候選挑選。Redis 不在本階段範圍內。

## 執行順序

1. `01_preflight.sql`：確認舊表和新表狀態。舊三表必須都是 0 筆。
2. `02_global_config.sql`
3. `03_pool_state.sql`
4. `04_kafka_raw_event.sql`：建立 RTP基本池專用 Kafka consumer、原始事件表與落地 MV。
5. `05_stats.sql`
6. `06_telemetry.sql`
7. `07_aggregation_checkpoint.sql`
8. `08_permissions.sql.template`：填入服務帳號後改名為 `.sql` 才可執行。
9. `09_postflight.sql`：跨 replicas 驗證物件、設定、Kafka consumer 與原始事件落地。
10. 所有 postflight 通過後，才可執行 `10_cleanup_old_tables.sql`。

`99_rollback_new_tables.sql` 只供新表尚未正式使用且資料可丟棄時緊急回退，不在正常執行順序內。

## Kafka 來源

QA 與 SG 使用相同的 ClickHouse/Kafka 結構與完整 `slotGame.winloseV1` payload schema。本包以 SG 線上 `icrown.ke_slotgame` 的實際 DDL為準：

- brokers：`172.31.15.82:9092,172.31.12.252:9092,172.31.5.49:9092`
- topic：`LCGAME.slotGame.winloseV1`
- RTP基本池專用 group：`rtp_basic_pool_sg_v1`
- consumers：8

專用 group 禁止改成既有 `clickhouse`，否則會和 `icrown.ke_slotgame` 搶 partition。

`08_permissions.sql.template` 必須填入：

- `__RTP_BASIC_POOL_SERVICE_USER__`：定時聚合服務
- `__RTP_BASIC_POOL_CONTROL_USER__`：操作全域 kill switch／每池 override 的控制服務

Kafka Engine 使用完整來源 schema；原始事件表只保存 RTP基本池需要的：

```text
GR_SEQ, AGT_Agent1, GM_GameCode, GameServer_Version,
GR_EndTime, GR_Bets, GR_Win, GR_BetType, IsTestRng,
Kafka topic / partition / offset
```

Kafka payload 沒有直接提供 `r` 與 `isBuyType`：

- `r`：`GameServer_Version` 為四位數 RTP 字串，例如 `0970 → 0.970`；不符合 `^0[0-9]{3}$` 的事件必須列為錯誤，不得轉成 0。
- `isBuyType`：依現行服務對照，`GR_BetType IN (1, 3)`；0 是一般、1 是 FeatureBuy、2 是 ExtraBet、3 是 SuperFeatureBuy。禁止以下注金額推測。

ClickHouse 保存 `_topic`、`_partition`、`_offset` 作為管線去重鍵；它不是業務 spin ID。Materialized View 依現行 RTP Monitor 的已驗證模式寫入 replicated local target。由於 Kafka commit 與 ClickHouse 寫入不是原子操作，聚合服務只讀 `rtp_basic_pool_raw_event_deduplicated`，由該 View 跨 shard 依 topic/partition/offset 去重。

## Shadow Mode 與 kill switch

- 全域設定的 `global_override_mode` 預設為 `FORCE_OFF`；Phase 1 不得改成 `AUTO`。
- 每池 `override_mode` 預設為 `AUTO`，但全域 `FORCE_OFF` 優先，確保結果一律單抽。
- Phase 2 只有經上線核准後，才可新增一版全域設定，把 `global_override_mode` 改為 `AUTO`。

## 聚合服務處理契約

1. 從 `rtp_basic_pool_aggregation_checkpoint_latest` 讀每個 topic/partition 的最後 offset。
2. 依各 partition 的 offset 遞增讀取 `rtp_basic_pool_raw_event_deduplicated`。
3. `IsTestRng = 1` 只保留於 raw event 稽核，不得更新正式 state/stats/telemetry。
4. 同一批內，以 pool key 分組後依 `GR_EndTime, GR_SEQ, kafka_partition, kafka_offset` 排序；每一筆依序計算 Dl/Bl/W/metric/band，不可先把整批 bet/win 聚合後才計算。
5. 寫入 pool state 的是完整最新 snapshot；重試同一批時必須寫出相同數值。
6. stats 只能寫本批「新增事件的增量」，且每批 INSERT 必須使用由 `topic/partition/from_offset/to_offset` 組成的 deterministic `insert_deduplication_token`，避免 checkpoint 寫入前崩潰造成重複累加。
7. telemetry 以 topic/partition/offset 為 key，重試會由 ReplacingMergeTree 收斂；`aggregation_run_id + processing_sequence` 保存實際處理順序，離線回放必須依此順序比對。
8. state、stats、telemetry 全部成功後，最後才寫 aggregation checkpoint；任一失敗都不得前移 checkpoint。

`override_mode` 與 Dl/Bl/W 位於同一狀態列。控制服務修改每池 override 時，必須與聚合服務共用同一個 per-key 序列化機制；聚合服務寫新 snapshot 前也必須保留最新 `override_mode`，不可用舊 snapshot 把人工設定蓋回去。

## 重要限制

- `05_stats.sql` 是主管要求的純統計表，只有三維度加 `bet/win`；它不能取代逐轉 telemetry。
- `05_stats.sql` 的查詢端只讀 `rtp_basic_pool_stats_totals`，不得直接假設 SummingMergeTree 已完成背景合併。
- `06_telemetry.sql` 是最新規格 v1 必需品，供 Shadow Mode 驗收、回放與後續調參。
- `last_seen` 的 30 天 TTL 是已定案的營運保留規則；不參與 Dl/Bl/W 的數學更新。
- 聚合服務必須由已去重的原始事件建立狀態與統計，不得直接對可能重送的 Kafka payload 重複累加。
- `01_preflight.sql`、`09_postflight.sql` 使用 `clusterAllReplicas`，DBA 執行帳號必須有 `READ ON REMOTE`。
- `10_cleanup_old_tables.sql` 含 DROP，只能在 preflight 全為 0 且 postflight 全部成功後執行。
- `99_rollback_new_tables.sql` 會刪除新物件與新資料，不能在正式使用後執行。
- raw event 與 telemetry 暫不設 TTL，因最新規格尚未決定其保留期；DBA 必須在正式執行前確認容量與保留政策。
