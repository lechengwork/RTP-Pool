# RTP 基本池 — DB 欄位需求（給 Server / Oscar）

> 對象：Server 工程師（Oscar）與其 AI。本文件回答「RTP 基本池要用哪些 DB 欄位、要新建哪些表」。
> 基準文件：《00_SG現況資料流與欄位契約》《02_RTP基本池控制_Server實作規格》。
> 依《00》§10 規定，分四段：已驗證現況 / 假設 / 設計決策 / 驗證計畫。
> 制定日：2026-09-16。

---

## 1. 已驗證現況（引用《00》，不重述細節）

- 歷史查詢入口為 `icrown.gr_slotgame_all`（Distributed）；金額在 `mv_slotgame` 已 `/10000` 正規化，讀 `gr_slotgame_all` 不可再除。若改讀 Kafka/`ke_slotgame` 才需 `/10000`。
- 每轉一列，含欄位：`AGT_Agent1`、`AGT_Agent3`、`GM_GameCode`、`GameServer_Version`、`GR_Currency`、`GR_Bets`、`GR_Win`、`GR_BetType`、`GR_EndTime`、`GR_SEQ`、`IsTestRNG`、`GR_FlagInterrupt` 等（型別見《00》§4、§5）。
- `GR_BetType` 對照（《00》§7）：0=一般、1=FeatureBuy（購買）、2=ExtraBet、3=SuperFeatureBuy（購買）。→ 與基本池規格的 `isBuyType` 定義一致：**1、3 為購買類型**。
- `GameServer_Version` 格式 `^0[0-9]{3}$`；近 24h 僅見 `0960`、`0970`（但我方細單樣本另見 0880/0920/0940/0970）。**不可用 `toFloat64OrZero` 硬轉**（《00》§7、規則 6）。
- 線上有 **18 種幣別**，跨幣別不可直接相加押注／派彩（《00》規則 3）。
- `AGT_Agent1` 與 `AGT_Agent3` 近 24h 為 41/41/41 一對一，但**非 schema 約束**（規則 4）。
- `gr_slotgame` order key = `AGT_Agent3, GM_GameCode, GR_EndTime, GR_SEQ`，保留 6 個月；Distributed 查詢**不保證全域排序**，需明確 `ORDER BY`（§6、§9-7）。
- 主要時間欄位為 `GR_EndTime`（結算時間，DB 時區 UTC-4）。

---

## 2. 假設（尚未由程式碼／DB／業務確認，請 Oscar 逐項回覆）

1. **對帳口徑為 `AGT_Agent1`（總代層級）。** 基本池規格的顆粒度是「(總代 × 遊戲 × RTP 版本)」，我方細單驗證亦以 `AGT_Agent1` 進行。若貴方對帳/既有工具是以 `AGT_Agent3` 為準，請告知，我們改 key。
2. **押注用 `GR_Bets`、派彩用 `GR_Win`。** 假設 `GR_Bets` 就是「RTP 目標 r 所對應的總押注基準」（含 ExtraBet/購買的加成後金額），且 `GR_Win` 為「該轉一次結算完成的完整派彩」。**需確認**：r（例如 0940→0.94）是對 `GR_Bets` 還是 `MathTotalBet` 定義？`GR_Win` 是否已含 Feature/Jackpot（是否需再加 `GR_Jackpot`）？此點若錯，整池 RTP 會系統性偏掉。
3. **目標 RTP `r` 來自一張權威對照表**（見 §3.6-Q），而非字串直轉。
4. **每轉可一次結算完成**（無跨轉 feature），與基本池規格前提一致。`GR_FlagInterrupt=1` 視為未正常結算 → 排除。
5. **即時 hot path + Shadow 兩者都要**：hot path 在遊戲 Server 端每轉決策（此處拿得到即時金額與版本）；Shadow/回放從 `gr_slotgame_all` 讀。

---

## 3. 設計決策

### 3.1 池 Key（**加入幣別**，四元組）

```
(AGT_Agent1, GM_GameCode, GameServer_Version, GR_Currency)
```

**為何加幣別**：基本池要累加 `Σ bet`、`Σ(r·bet − win)`。這些是金額相加，**跨幣別相加無意義**（《00》規則 3）。metric 雖是比例，但分子分母若混幣別即失真。加入幣別後，每個池都是單一幣別、金額可比。代價是池數 ×（幣別數），低量幣別的池資料較少 → 但動態帶寬與暖機機制正好會自動給它較寬鬆的控制，符合設計。

> 不採用「換匯正規化成單一幣別」：匯率浮動、增加資料相依，且違反《00》規則 3 的精神。

### 3.2 每轉輸入欄位契約（演算法要讀的既有欄位）

| 演算法用途 | 來源欄位 | 說明 |
|---|---|---|
| 池 key－總代 | `AGT_Agent1` | 見 §2-1 假設 |
| 池 key－遊戲 | `GM_GameCode` | |
| 池 key－RTP 版本 | `GameServer_Version` | 先驗格式 `^0[0-9]{3}$` |
| 池 key－幣別 | `GR_Currency` | |
| `bet` | `GR_Bets` | 讀 `gr_slotgame_all` 已 `/10000`；讀 Kafka 需 `/10000` |
| `win` | `GR_Win` | 同上；需確認是否含 Jackpot（§2-2） |
| `isBuyType` | `GR_BetType ∈ {1,3}` | 1、3=購買；只記帳不挑選 |
| 目標 `r` | 由 `GameServer_Version`(＋`GM_GameCode`) 查權威 RTP 表 | 不可硬轉 |
| 逐轉排序（Shadow） | `GR_EndTime`, `GR_SEQ` | 見 §3.5 |
| 資料清洗 | `IsTestRNG`, `GR_FlagInterrupt`, `GR_Bets` | 見 §3.5 |

### 3.3 池狀態儲存（**建議放 KV／交易型 DB，不是 ClickHouse**）

池狀態是「每轉 read-modify-write 的可變狀態」，屬低延遲點更新，**不適合 ClickHouse（MergeTree 是 append 導向）**。建議放 Redis 或交易型 DB（規格 §9.2 的一致性方案在此落地）。每個池 key 一筆：

| 欄位 | 型別 | 初始 | 說明 |
|---|---|---|---|
| `agent1` | String | — | key |
| `game_code` | String | — | key |
| `rtp_version` | String | — | key（例 `0940`） |
| `currency` | String | — | key |
| `dl` | Float64 (double) | 0.0 | 加權水位 Σ(r·bet − win) |
| `bl` | Float64 | 0.0 | 加權下注 Σ bet |
| `w` | Float64 | 0.0 | 加權轉數 |
| `override_mode` | Enum8 | `AUTO` | `AUTO`/`FORCE_ON`/`FORCE_OFF` |
| `state_version` | UInt64 | 0 | 樂觀鎖／並發防護（規格 §9.2） |
| `updated_at` | DateTime64(6,'Etc/GMT+4') | — | 最後更新 |

主鍵＝四元組 key。`dl/bl/w` 三者用同一 λ 每轉衰減；上限有界（`w → 1/(1−λ) ≈ 400k`），不會溢位。

### 3.4 事件 log（**放 ClickHouse，append-only，一轉一列**）

這是規格 §11 要求的原始 log，型態跟 `gr_slotgame` 一樣是 append，適合 ClickHouse（可比照既有 Kafka→CH 管線）。**v1 就要落地**（事後補不回來）。建議欄位：

| 欄位 | 型別 | 說明 |
|---|---|---|
| `event_time` | DateTime64(6,'Etc/GMT+4') | = `GR_EndTime` |
| `gr_seq` | UInt64 | 對應來源注單，可回溯 |
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
| `dl_after`/`bl_after`/`w_after` | Float64 | （選填）更新後狀態，利於回放稽核 |

建議 Partition `toYYYYMMDD(event_time)`、Order `(agent1, game_code, rtp_version, currency, event_time, gr_seq)`。

### 3.5 資料清洗規則（進池前）

| 情況 | 判斷 | 處理 |
|---|---|---|
| 測試 RNG | `IsTestRNG=1` | **排除**：不更新池、不記 log（或記但標記排除） |
| 中斷未結算 | `GR_FlagInterrupt=1` | **排除**：不更新池 |
| 零／負押注 | `GR_Bets<=0` | **跳過**：不更新池（避免除零、無意義） |
| 負派彩 | `GR_Win<0` | 跳過並告警（正常不應出現） |
| 非法版本 | 不符 `^0[0-9]{3}$` 或不在 RTP 表 | **跳過控制並告警**：無法得知 r 即無法記帳 |

排序（Shadow/回放）：`ORDER BY AGT_Agent1, GM_GameCode, GameServer_Version, GR_Currency, GR_EndTime, GR_SEQ`。Hot path 則以 spin 實際到達順序，同一 key 需序列化或最終一致（規格 §9.2）。

### 3.6 對《00》§9 十問的逐點回答

1. **粒度 Agent1 或 Agent3？** → `AGT_Agent1`（對帳在總代層級；細單驗證亦用 Agent1）。待業務確認（§2-1）。
2. **幣別是否入 key？** → **入 key**（§3.1）。不做跨幣別換算。
3. **押注用哪個欄位？** → `GR_Bets`（待確認 r 是否對 `GR_Bets` 定義，否則改 `MathTotalBet`，§2-2）。
4. **`GR_Win` 是否含所有 Jackpot/Feature？** → 假設含；**請 Oscar 確認**是否需加 `GR_Jackpot`（§2-2）。
5. **購買玩法如何處理？** → `GR_BetType∈{1,3}` 記帳但不參與候選挑選（規格 §8）。
6. **`IsTestRNG`/中斷/零押注/非法版本？** → 見 §3.5。
7. **逐轉排序依據？** → `GR_EndTime, GR_SEQ`（§3.5）。
8. **需多長歷史窗？** → Shadow/回放用 `gr_slotgame_all` 6 個月即足；池狀態本身另存、不需回溯重算。
9. **一致性語意？** → 池狀態允許最終一致（統計控制器容忍雜訊，規格 §9.2）；事件 log 至少 at-least-once、可容忍極小重複。
10. **Shadow 還是 hot path？** → **兩者都要**，先 Shadow（規格 §13 第 1 階段）再開挑選；架構見規格 §13。

> 額外需求：需要一張**權威 RTP 對照表**（key：`GameServer_Version` 或 `(GM_GameCode, GameServer_Version)` → 目標 r），供 hot path 與回放共用，取代字串直轉。

---

## 4. 驗證計畫

1. **回放對帳**：以 `gr_slotgame_all` 一段真實資料，按 §3.5 排序離線重算 `dl/bl/w/metric/band`，與線上池狀態逐筆比對（相對誤差 < 1e-9）。
2. **金額口徑**：抽樣核對 `gr_slotgame_all` 的 `GR_Bets/GR_Win` 是否已 `/10000`，且與 §2-2 確認結果一致（尤其購買/ExtraBet 的 bet 基準）。
3. **單幣別 RTP 核對**：對單一 (agent1, game, version, currency) 池，用該幣別加總 `Σwin/Σbet` 對照該版本目標 r，確認 r 定義正確（不跨幣別）。
4. **邊界案例**：測試 RNG、中斷、零押注、非法/新版本、極端大獎（正常轉高倍）各建一筆，驗證 §3.5 行為。
5. **Shadow 零影響證明**：第 1 階段所有結果與「關閉系統」逐筆相同（規格 §13 第 1 階段驗收）。
6. **並發正確**：同一 key 高並發壓測，池狀態不錯亂（對照序列化基準）。
