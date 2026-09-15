# RTP基本池 ClickHouse 遷移包（DBA 審閱版）

**狀態：僅供審閱，尚未執行。**本包依 `規格文件/02_RTP基本池控制_Server實作規格.md` 的 Phase 1 Shadow Mode 設計。

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
4. `04_kafka_raw_event.sql.template`：DBA 先填妥 Kafka placeholders，審閱後改名為 `.sql` 才能執行。
5. `05_stats.sql`
6. `06_telemetry.sql`
7. `07_aggregation_checkpoint.sql`
8. `08_permissions.sql.template`：填入服務帳號後才可執行。
9. 確認新表建妥、Kafka 消費正常後，才可執行 `09_cleanup_old_tables.sql`。

## Kafka template 必填值

`04_kafka_raw_event.sql.template` 中以下 placeholder 必須由 DBA／Kafka owner 填入：

- `__KAFKA_BROKERS__`
- `__RTP_BASIC_POOL_TOPIC__`
- `__RTP_BASIC_POOL_CONSUMER_GROUP__`
- `__KAFKA_NUM_CONSUMERS__`

`08_permissions.sql.template` 還必須填入：

- `__RTP_BASIC_POOL_SERVICE_USER__`：定時聚合服務
- `__RTP_BASIC_POOL_CONTROL_USER__`：操作全域 kill switch／每池 override 的控制服務

Kafka 訊息格式為 `JSONEachRow`，每筆至少要有：

```text
AGT_Agent1, GM_GameCode, GameServer_Version,
event_at, bet, win, r, is_buy_type
```

ClickHouse 會保存 `_topic`、`_partition`、`_offset`，作為原始事件的管線去重鍵；它不是業務 spin ID。

Producer 必須使用 `AGT_Agent1 × GM_GameCode × GameServer_Version` 作為 message key，保證同一個池固定進入同一 Kafka partition。Materialized View 經 Distributed 表寫入，讓相同 topic/partition 固定路由到相同 ClickHouse shard，避免 rebalance 後跨 shard 無法去重。

## Shadow Mode 與 kill switch

- 全域設定的 `global_override_mode` 預設為 `FORCE_OFF`；Phase 1 不得改成 `AUTO`。
- 每池 `override_mode` 預設為 `AUTO`，但全域 `FORCE_OFF` 優先，確保結果一律單抽。
- Phase 2 只有經上線核准後，才可新增一版全域設定，把 `global_override_mode` 改為 `AUTO`。

## 聚合服務處理契約

1. 從 `rtp_basic_pool_aggregation_checkpoint_latest` 讀每個 topic/partition 的最後 offset。
2. 依 `kafka_partition, kafka_offset` 遞增讀取 `rtp_basic_pool_raw_event FINAL`；同一 pool key 因 Kafka message key 固定在同一 partition，事件順序穩定。
3. 每一筆事件都依序計算 Dl/Bl/W/metric/band，不可先把整批 bet/win 聚合後才計算。
4. 寫入 pool state 的是完整最新 snapshot；重試同一批時必須寫出相同數值。
5. stats 只能寫本批「新增事件的增量」，且每批 INSERT 必須使用由 `topic/partition/from_offset/to_offset` 組成的 deterministic `insert_deduplication_token`，避免 checkpoint 寫入前崩潰造成重複累加。
6. telemetry 以 topic/partition/offset 為 key，重試會由 ReplacingMergeTree 收斂。
7. state、stats、telemetry 全部成功後，最後才寫 aggregation checkpoint；任一失敗都不得前移 checkpoint。

## 重要限制

- `05_stats.sql` 是主管要求的純統計表，只有三維度加 `bet/win`；它不能取代逐轉 telemetry。
- `05_stats.sql` 的查詢端只讀 `rtp_basic_pool_stats_totals`，不得直接假設 SummingMergeTree 已完成背景合併。
- `06_telemetry.sql` 是最新規格 v1 必需品，供 Shadow Mode 驗收、回放與後續調參。
- `last_seen` 的 30 天 TTL 是已定案的營運保留規則；不參與 Dl/Bl/W 的數學更新。
- 聚合服務必須由已去重的原始事件建立狀態與統計，不得直接對可能重送的 Kafka payload 重複累加。
- `09_cleanup_old_tables.sql` 含 DROP，只能在 preflight 全為 0 且新表驗證成功後執行。
