# RTP基本池 MVP · DBA 執行包

本資料夾只處理 Phase 1 Shadow Mode：只記帳、只觀測，不做候選挑選；Redis 不在本階段。

## 最小資料流

```text
資料來源（後續接 Kafka）
    → rtp_basic_pool_event_log
    → 排程服務更新 rtp_basic_pool_state

rtp_basic_pool_stats 是 event log 的即時計算 View，不另存統計副本。
```

## 執行順序

1. `01_preflight.sql`
2. `02_event_log.sql`
3. `03_pool_state.sql`
4. 執行 `04_verify.sql`，確認物件可查；目前沒有資料來源時 0 筆是正常結果。
5. 新表確認正常後，才執行 `05_cleanup_old_tables.sql`。

## MVP 物件

- `rtp_basic_pool_event_log`：同時保存來源事實與規格要求的 telemetry。
- `rtp_basic_pool_event_log_latest`：依池 key 與 `GR_SEQ` 取得事件最新版本。
- `rtp_basic_pool_state`：保存每池 `Dl / Bl / W / override_mode`。
- `rtp_basic_pool_stats`：主管要求的三維度加 `bet / win` 統計 View。

規格公式中的 `r` 在資料庫命名為 `target_rtp`，由 `GameServer_Version` 推導，例如 `0970 → 0.970`；格式非法時為 `NULL`，不得轉成 0。

`is_buy_type` 由 `GR_BetType` 判定：0=一般、1=FeatureBuy、2=ExtraBet、3=SuperFeatureBuy；1、3 是購買玩法，只記帳、不參與候選挑選。

`IsTestRng = 1` 只保留在 event log，不得更新正式池狀態或統計。

## 全域參數

MVP 放在服務設定，不建設定表：

```text
LAMBDA=0.9999975
EPSILON=0.05
K=3
VOL=15
W_MIN=10000
N=10
GLOBAL_MODE=FORCE_OFF
```

Phase 1 的 `GLOBAL_MODE` 必須維持 `FORCE_OFF`。

## 本階段不做

- Kafka Engine、consumer group 與 Kafka MV
- Redis
- DB 全域設定表
- 獨立 checkpoint 表
- 獨立 telemetry 表
- 權限角色細分
- Dashboard、告警、管理 API
- Steering 與候選挑選

這些項目等 MVP 的資料與服務流程穩定後再加，避免先綁死頻繁變更中的設計。
