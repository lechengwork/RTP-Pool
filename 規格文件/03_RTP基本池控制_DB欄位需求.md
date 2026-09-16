# RTP 基本池 — DB 欄位說明

> 對象：Server 工程師與其 AI。本文件定義 RTP 基本池所需的 DB 欄位與要新建的表。
> 日期：2026-09-16。

---

## 1. Pool Key

每個池以四元組為主鍵：

```
(AGT_Agent1, GM_GameCode, GameServer_Version, GR_Currency)
```

- **AGT_Agent1**：總代（對帳在總代層級）。
- **GM_GameCode**：遊戲。
- **GameServer_Version**：RTP 版本（例 `0940`）。
- **GR_Currency**：幣別。**必須入 key**——跨幣別金額不可相加，每個池須為單一幣別。

---

## 2. 每轉輸入欄位（讀既有 `gr_slotgame_all`）

| 用途 | 欄位 | 說明 |
|---|---|---|
| Key－總代 | `AGT_Agent1` | |
| Key－遊戲 | `GM_GameCode` | |
| Key－版本 | `GameServer_Version` | 先驗格式 `^0[0-9]{3}$` |
| Key－幣別 | `GR_Currency` | |
| `bet` | `GR_Bets` | 讀 `gr_slotgame_all` 已 `/10000`；讀 Kafka/`ke_slotgame` 需 `/10000` |
| `win` | `GR_Win` | 已含完整 Jackpot/Feature，直接用，不加減 |
| `isBuyType` | `GR_BetType ∈ {1,3}` | 1=FeatureBuy、3=SuperFeatureBuy；記帳但不參與候選挑選 |
| 目標 `r` | 由 `GameServer_Version` 推導 | 驗格式後 `r = toInt(version)/1000`（`0970→0.970`） |
| 逐轉排序 | `GR_EndTime`, `GR_SEQ` | Shadow/回放用；Distributed 查詢須明確 `ORDER BY` |
| 清洗 | `IsTestRNG`, `GR_FlagInterrupt`, `GR_Bets` | 見 §5 |

---

## 3. Pool State 表（新建，KV／交易型 DB）

每轉 read-modify-write 的可變狀態，屬低延遲點更新，放 Redis 或交易型 DB（不放 ClickHouse）。每個 Pool Key 一筆：

| 欄位 | 型別 | 初始 | 說明 |
|---|---|---|---|
| `agent1` | String | — | key |
| `game_code` | String | — | key |
| `rtp_version` | String | — | key（例 `0940`） |
| `currency` | String | — | key |
| `dl` | Float64 (double) | 0.0 | 加權水位 Σ(r·bet − win) |
| `bl` | Float64 | 0.0 | 加權下注 Σ bet |
| `w` | Float64 | 0.0 | 加權轉數 |
| `override_mode` | Enum8 | `AUTO` | `AUTO` / `FORCE_ON` / `FORCE_OFF` |
| `updated_at` | DateTime64(6,'Etc/GMT+4') | — | 最後更新 |

主鍵＝四元組 key。`dl/bl/w` 三者用同一 λ 每轉衰減；上限有界（`w → 1/(1−λ) ≈ 400k`），不會溢位。

---

## 4. 事件 Log 表（新建，ClickHouse append-only）

一轉一列，供調參與稽核。Partition `toYYYYMMDD(event_time)`；Order `(agent1, game_code, rtp_version, currency, event_time, gr_seq)`。

| 欄位 | 型別 | 說明 |
|---|---|---|
| `event_time` | DateTime64(6,'Etc/GMT+4') | = `GR_EndTime` |
| `gr_seq` | UInt64 | 對應來源注單 |
| `agent1` / `game_code` / `rtp_version` / `currency` | String / LowCardinality | 池 key |
| `bet_type` | UInt8 | 原始 `GR_BetType` |
| `is_buy_type` | UInt8 | 1、3→1 |
| `r` | Float64 | 該轉採用的目標 RTP |
| `bet` | Decimal(18,4) | 正規化後押注 |
| `win` | Decimal(18,4) | 正規化後派彩 |
| `metric_before` | Float64 | 當轉決策前的 Dl/Bl |
| `band` | Float64 | 當轉動態帶寬 |
| `control_on` | UInt8 | 是否啟用控制 |
| `triggered` | UInt8 | 是否實際走候選挑選 |
| `n_candidates` | UInt16 | 觸發時的 N；未觸發 0 |
| `chosen_win` | Decimal(18,4) | 最終選中結果派彩（＝win） |
| `dl_after` / `bl_after` / `w_after` | Float64 | （選填）更新後狀態，利於回放稽核 |

---

## 5. 資料清洗規則（進池前）

| 情況 | 判斷 | 處理 |
|---|---|---|
| 測試 RNG | `IsTestRNG = 1` | 整筆排除，不進池 |
| 中斷未結算 | `GR_FlagInterrupt = 1` | 整筆排除，不進池 |
| 零／負押注 | `GR_Bets <= 0` | 跳過，不進池 |
| 負派彩 | `GR_Win < 0` | 跳過並告警（正常不應出現） |
| 非法版本 | 不符 `^0[0-9]{3}$`（如 `"rng"`） | 整筆跳過、不進池的任何流程（不推導 r、不記帳、不控制），只記告警 |

排序（Shadow/回放）：`ORDER BY AGT_Agent1, GM_GameCode, GameServer_Version, GR_Currency, GR_EndTime, GR_SEQ`。

---

## 6. 並發

- **累加 `dl/bl/w`**：用原子遞增（atomic add）→ 無 lost update、無鎖。
- **決策讀取**：允許讀到稍舊的快照（控制器容忍雜訊）。
- 因此不需要樂觀鎖／版本欄位。
