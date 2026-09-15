# DBA 包驗證紀錄

驗證日期：2026-09-15。目標：SG ClickHouse 26.3.9.8。

## 已確認

- 最新 Server 規格 commit：`e661081`。
- SG ClickHouse 連線與 `sg_cluster` 名稱可用。
- `sg_cluster` 目前為 2 shards × 2 replicas（共 4 nodes）。
- 線上 `icrown.ke_slotgame` 為 Kafka Engine，完整 schema 已逐欄對齊 `04_kafka_raw_event.sql`。
- SG Kafka brokers：`172.31.15.82:9092,172.31.12.252:9092,172.31.5.49:9092`。
- Topic：`LCGAME.slotGame.winloseV1`。
- 既有 group 是 `clickhouse`；新包使用獨立 `rtp_basic_pool_sg_v1`，不與既有消費者搶 partition。
- 本機 replica 的 `ke_slotgame` 有 8 個 consumers，當下分配 partitions 32–39，且持續 poll/commit；新 group 沿用每節點 8 consumers。
- SG 最近一天共有 14,762,071 筆 slot rows；`IsTestRng=1`、零押注、負押注、負贏分、中斷、非法 RTP version 均為 0。
- 最近一天 `GR_BetType` 只有 0 與 1；現行程式正式對照為 0=一般、1=FeatureBuy、2=ExtraBet、3=SuperFeatureBuy，購買類型是 1 或 3。
- 舊三張 `outcome_safety_net_*` 邏輯表目前都是 0 筆。
- 所有 `CREATE TABLE` 與 Kafka Engine DDL 已通過目標 ClickHouse `EXPLAIN SYNTAX`。
- Kafka MV 的 SELECT 已對線上 `ke_slotgame` schema 通過 `EXPLAIN SYNTAX`。
- 全包沒有 `SELECT *`。

## 尚未解除的執行阻擋

1. 目前查詢帳號缺少 `READ ON REMOTE`，尚未跨所有 replicas 確認 topic 的完整 partition 分配；DBA 必須執行 `01_preflight.sql`。
2. Docker Desktop 未啟動，尚未在拋棄式 ClickHouse 26.3.9.8 完整實建整包；目前是 parser/schema 級驗證，不是部署證明。
3. `__RTP_BASIC_POOL_SERVICE_USER__` 與 `__RTP_BASIC_POOL_CONTROL_USER__` 尚未提供，`08_permissions.sql.template` 不可直接執行。
4. 新 Kafka group 尚未建立，實際消費、MV 落地、rebalance、重送去重與錯誤訊息行為尚未做 runtime 驗收。
5. 聚合服務尚未實作，逐事件排序、checkpoint、deterministic insert token、per-key override 競態與離線回放尚未驗證。
6. raw event 與 telemetry 的保留期／容量尚未由產品與 DBA 定案，目前刻意不設 TTL。

## 可交付 DBA 的條件

- 上述第 1～3 項完成後，DBA 才能開始建表。
- 建表後必須執行 `09_postflight.sql`，確認所有 replicas 的物件、consumer、config 與 raw event 落地。
- `09_postflight.sql` 成功且 raw offset 無重複後，才可執行 `10_cleanup_old_tables.sql`。
- 第 4～6 項完成前，只能視為 Shadow Mode 基礎設施，不能宣稱 RTP基本池可上線或可啟用 steering。
