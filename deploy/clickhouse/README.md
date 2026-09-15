# Outcome Safety Net Pool · ClickHouse DDL

這個目錄只屬於 Outcome Safety Net Pool 的 Phase 1，與既有 RTP Monitor 表無關；不得修改或重用 RTP Monitor 的既有表。

## 已定案

### 1. 池的 key

每一個安全網池由下列三維度唯一決定：

- `AGT_Agent1`：Agent 1 GUID
- `GM_GameCode`：遊戲代碼
- `GameServer_Version`：RTP 版本字串，保留前導零，例如 `0970`

目標 RTP `r` 不落表，由 RTP 版本在執行期推導。

### 2. 池狀態表

`001_outcome_safety_net_pool_state.sql` 建立每池目前狀態：

- `AGG_DecayedBetSum`：衰減後累積押注
- `AGG_DecayedRtpDiff`：衰減後 RTP 差額
- `AG_SpinCount`：納入安全網的累積轉數
- `last_seen`：最後一筆納入安全網的時間
- `epsilon_override`：個別池 ε 覆寫；`NULL` 時使用全域 ε

狀態更新以新版本 `INSERT` 寫入 `ReplicatedReplacingMergeTree`；同一池的 read-modify-write 必須由應用層序列化。

**TTL 決策是 30 天，以 `last_seen` 為準。**目前 `001` 與線上
`outcome_safety_net_pool_state_local` 均為 30 天，符合規格文件與目前定案。

### 3. 純統計表

`002_outcome_safety_net_stats.sql` 是純累積統計，只有：

| 類別 | 欄位 |
|---|---|
| 維度 | `AGT_Agent1`、`GM_GameCode`、`GameServer_Version` |
| 數值 | `bet`、`win` |

它使用 `ReplicatedSummingMergeTree` 加總 `bet`、`win`。本表不保存逐轉資料，因此不含 `spin_id`、時間、`accounting_bucket`、去重或版本欄位。

### 4. 全域設定表

`003_outcome_safety_net_global_config.sql` 保存全域：

- `lambda`
- `epsilon`
- `warmup_spin_count`
- `candidate_count`（K）

此檔只建立結構，尚未插入參數。實際值必須先以歷史資料回放校準，不得自行假設。

## 暫不做

- 記債池與客戶需求／還款流程（Phase 2）
- `accounting_bucket` 欄位
- 逐轉帳本、`spin_id`、逐轉冪等或回放表
- 任何 RTP Monitor 既有表的修改

## 已部署狀態（2026-09-15）

| 線上物件 | 是否存在 | 筆數 |
|---|---:|---:|
| `outcome_safety_net_pool_state` | 是 | 0 |
| `outcome_safety_net_global_config` | 是 | 0 |
| `outcome_safety_net_spin_ledger`（舊 002） | 是 | 0 |
| `outcome_safety_net_stats`（新 002） | 否 | — |

## 舊 002 的遷移

`004_migrate_outcome_safety_net_spin_ledger_to_stats.sql` 會移除舊的
`outcome_safety_net_spin_ledger`／`_local`，並建立新的純統計表。

此遷移會 `DROP TABLE`。只有 preflight 查詢仍回傳 `row_count = 0` 時才能執行；若已有資料，必須先改為資料保留的彙總搬遷，不能直接執行 004。
