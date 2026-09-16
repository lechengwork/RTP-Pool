# SG 現況資料流與欄位契約（給數學工程師與 AI）

> 用途：在設計 RTP、波動、分群、回放或池子演算法前，先理解 SG 線上目前真正存在的資料來源、轉換、粒度與限制。
>
> 本文件只記錄已部署且已查證的現況，不描述任何尚未上線的 RTP-Pool 設計，也不代表新功能需求。
>
> 查證日期：2026-09-16（台北時間）。資料庫欄位時區為 `Etc/GMT+4`，即 UTC-4，不是 Asia/Taipei。

## 1. AI 使用規則

使用本文件作為設計上下文時，必須遵守：

1. 將「線上已驗證事實」和「新設計建議」分開；不得把建議寫成現況。
2. 不得假設 Kafka 金額已是實際金額；`ke_slotgame` 金額需依既有 MV `/10000` 後才與 `gr_slotgame` 一致。
3. 不得跨幣別直接加總押注／贏分後宣稱是整體 RTP；目前有 18 種幣別。
4. 不得把 `AGT_Agent1` 與 `AGT_Agent3` 永久視為一對一。最近 24 小時為 41／41／41 pairs，只是觀測，不是 schema 約束。
5. 不得直接查 Kafka Engine 當歷史表；歷史查詢使用 `gr_slotgame_all`。
6. 不得假設 `GameServer_Version` 永遠可轉數字；使用前先驗證格式或查正式 RTP 對照。
7. 不得把現有 RTP Monitor 聚合表當成任何新演算法的預設資料源；先核對粒度是否相符。

## 2. 已部署資料流

```text
Kafka topic
LCGAME.slotGame.winloseV1
        │
        ▼
icrown.ke_slotgame
Kafka Engine，group = clickhouse
        │
        ▼
icrown.mv_slotgame
金額 /10000、型別與精度轉換
        │
        ▼
icrown.gr_slotgame
ReplicatedMergeTree，本地 shard 資料，保留 6 個月
        │
        ▼
icrown.gr_slotgame_all
Distributed，全 SG 查詢入口
```

另外，既有 RTP Monitor 從 `icrown.gr_slotgame` 建立自己的分鐘聚合：

```text
gr_slotgame
   → rtp_monitor_minute_mv_pressure
   → rtp_monitor_minute_pressure_local
   → rtp_monitor_minute_pressure
```

RTP Monitor 分鐘表的粒度是：

```text
分鐘 × AGT_Agent3 × 遊戲 × RTP版本 × BetType × 幣別
```

其中 `AGT_Agent1` 是以 `max` 保存的屬性，不是該表的分組 key。因此，若新需求的 key 是 `AGT_Agent1 × 遊戲 × RTP版本`，不能在未驗證映射與聚合語意前直接套用 RTP Monitor 分鐘表。

## 3. SG ClickHouse 與 Kafka 現況

| 項目 | 線上值 |
|---|---|
| ClickHouse | `26.3.9.8` |
| Database | `icrown` |
| Cluster | `sg_cluster` |
| Topology | 2 shards × 2 replicas，共 4 nodes |
| Kafka brokers | `172.31.15.82:9092,172.31.12.252:9092,172.31.5.49:9092` |
| Kafka topic | `LCGAME.slotGame.winloseV1` |
| 既有 consumer group | `clickhouse` |
| Kafka format | `JSONEachRow` |
| Consumers | 每張 Kafka Engine table、每節點 8 |

唯讀檢查時，本機 replica 的 `ke_slotgame` consumers 正在使用 partitions 32～39，`is_currently_used=1`、`missing_dependencies=[]`，poll、commit 與 offset 都持續前進。這證明 topic 與既有消費鏈在線；完整跨 replica assignments 需由具 `READ ON REMOTE` 的 DBA 帳號查證。

## 4. `ke_slotgame` 完整 payload schema

`ke_slotgame` 是 Kafka Engine，包含 40 欄：

| # | 欄位 | Kafka 型別 | 類別／用途 |
|---:|---|---|---|
| 1 | `GR_SEQ` | `UInt64` | 來源注單／遊戲序號；是否全域唯一未由 schema 保證 |
| 2 | `GM_GameCode` | `LowCardinality(String)` | 遊戲代碼 |
| 3 | `AGT_Agent1` | `LowCardinality(String)` | Agent 第 1 層 ID |
| 4 | `AGT_Agent2` | `LowCardinality(String)` | Agent 第 2 層 ID |
| 5 | `AGT_Agent3` | `LowCardinality(String)` | Agent 第 3 層 ID；現有 RTP Monitor 的判定 key |
| 6 | `PLY_GUID` | `String` | 玩家 GUID |
| 7 | `AGT_AccountID` | `LowCardinality(String)` | Agent 帳號識別 |
| 8 | `PLY_AccountID` | `String` | 玩家帳號識別 |
| 9 | `GR_Currency` | `LowCardinality(String)` | 幣別；跨幣別不可直接相加 |
| 10 | `GR_Bets` | `Decimal(22,4)` | 原始押注值；落地時 `/10000` |
| 11 | `GR_GamebleBets` | `Decimal(22,4)` | 原始 Gamble bet；落地時 `/10000` |
| 12 | `GR_Win` | `Decimal(22,4)` | 原始派彩；落地時 `/10000` |
| 13 | `GR_ValidBets` | `Decimal(22,4)` | 原始有效押注；落地時 `/10000` |
| 14 | `GR_NetWin` | `Decimal(22,4)` | 原始淨輸贏；落地時 `/10000` |
| 15 | `GR_Jackpot` | `Decimal(22,4)` | 原始 Jackpot；落地時 `/10000` |
| 16 | `GR_JackpotType` | `UInt8` | Jackpot 類型 |
| 17 | `GR_JackpotContribute` | `Decimal(22,4)` | 原始 Jackpot contribution；落地時 `/10000` |
| 18 | `GR_Record` | `String` | 遊戲紀錄內容 |
| 19 | `GR_FlagFreeGame` | `Bool` | Free Game 標記 |
| 20 | `GR_FlagGamble` | `Bool` | Gamble 標記 |
| 21 | `GR_FlagInterrupt` | `Bool` | 中斷標記 |
| 22 | `GR_StartTime` | `DateTime64(9, 'Etc/GMT+4')` | 開始時間 |
| 23 | `GR_EndTime` | `DateTime64(9, 'Etc/GMT+4')` | 結算時間；主要時間欄位 |
| 24 | `GR_IPAddress` | `String` | IP |
| 25 | `GameServer_Version` | `LowCardinality(String)` | RTP／Server 版本字串 |
| 26 | `GameServer_Name` | `LowCardinality(String)` | Game Server 名稱 |
| 27 | `GR_BeforeBalance` | `Decimal(22,4)` | 原始下注前餘額；落地時 `/10000` |
| 28 | `GR_AfterBalance` | `Decimal(22,4)` | 原始下注後餘額；落地時 `/10000` |
| 29 | `GR_CreateDateTime` | `DateTime64(9, 'Etc/GMT+4')` | 建立時間 |
| 30 | `GR_ClientType` | `UInt8` | Client 類型 |
| 31 | `GR_BetType` | `UInt8` | 下注玩法類型 |
| 32 | `GR_RecordCompress` | `String` | 壓縮遊戲紀錄 |
| 33 | `Multiplier` | `Decimal(10,4)` | 倍數 |
| 34 | `GitVersion` | `LowCardinality(String)` | 程式版本 |
| 35 | `GitCommitHash` | `FixedString(40)` | Git commit hash |
| 36 | `MathBet` | `Decimal(18,4)` | 原始 math bet；落地時 `/10000` |
| 37 | `MathTotalBet` | `Decimal(22,4)` | 原始 math total bet；落地時 `/10000` |
| 38 | `IsTestRng` | `Bool` | 測試 RNG 標記 |
| 39 | `Extend` | `Array(Int64)` | 擴充資料；落地後轉成 String |
| 40 | `UsedRng` | `Array(Int64)` | 已使用 RNG；落地後轉成 String |

## 5. `mv_slotgame` 實際轉換

Kafka payload 不會原樣進入歷史表。現有 `mv_slotgame` 執行：

### 5.1 金額正規化

下列欄位全部除以 10,000：

```text
GR_Bets
GR_GamebleBets
GR_Win
GR_ValidBets
GR_NetWin
GR_Jackpot
GR_JackpotContribute
GR_BeforeBalance
GR_AfterBalance
MathBet
MathTotalBet
```

因此：

- 從 `ke_slotgame`／Kafka 直接讀時，必須套用 `/10000`。
- 從 `gr_slotgame`／`gr_slotgame_all` 讀時，金額已正規化，不得再除一次。

### 5.2 其他轉換

| Kafka | 落地 `gr_slotgame` |
|---|---|
| `DateTime64(9)` | `DateTime64(6)` |
| Flag `Bool` | `UInt8` |
| `IsTestRng Bool` | `IsTestRNG UInt8` |
| `Extend Array(Int64)` | `Extend String` |
| `UsedRng Array(Int64)` | `UsedRNG String` |
| 金額 `Decimal(22,4)` | 多數為 `Decimal(18,4)` |

## 6. `gr_slotgame`／`gr_slotgame_all`

### `gr_slotgame`

- Engine：`ReplicatedMergeTree`
- Partition：`toYYYYMMDD(GR_EndTime)`
- Order key：`AGT_Agent3, GM_GameCode, GR_EndTime, GR_SEQ`
- TTL：6 個月
- 每個 shard 的本地資料

### `gr_slotgame_all`

- Engine：`Distributed('sg_cluster', 'icrown', 'gr_slotgame', cityHash64(PLY_GUID))`
- 跨 SG shards 的一般查詢入口
- Query 不保證天然依時間或 `GR_SEQ` 排序；需要明確 `ORDER BY`

## 7. 欄位語意與目前程式對照

### RTP version

最近 24 小時只觀察到：

```text
0960
0970
```

兩者均符合 `^0[0-9]{3}$`。如果設計以版本推導目標 RTP，可得到 `0960 → 0.960`、`0970 → 0.970`；但新版本上線時仍應驗證格式，不能使用 `toFloat64OrZero` 把非法值默默轉成 0。

### BetType

現行應用程式對照：

| `GR_BetType` | 語意 | 是否購買類型 |
|---:|---|---:|
| 0 | 一般 | 否 |
| 1 | FeatureBuy | 是 |
| 2 | ExtraBet | 否 |
| 3 | SuperFeatureBuy | 是 |

最近 24 小時只觀察到 0、1；不代表 2、3 永遠不會出現。

## 8. 最近 24 小時線上快照

觀測時間（DB 時區 UTC-4）：`2026-09-15 21:30:02.956199`。

| 指標 | 值 |
|---|---:|
| rows | 15,132,603 |
| `AGT_Agent1` distinct | 41 |
| `AGT_Agent3` distinct | 41 |
| Agent1/Agent3 distinct pairs | 41 |
| games | 36 |
| RTP versions | 2 |
| currencies | 18 |
| BetType 0 rows | 15,103,095 |
| BetType 1 rows | 29,310 |
| zero bet | 0 |
| negative bet | 0 |
| negative win | 0 |
| test RNG | 0 |
| interrupted | 0 |
| invalid RTP version | 0 |

重要：上述分布只是一日快照，不是永久資料約束。因存在 18 種幣別，本文件刻意不提供跨幣別加總的 RTP，避免用不可比較的貨幣數值做錯誤結論。

## 9. 數學設計前必須先回答的問題

在開新規格前，至少明確回答：

1. 計算粒度要用 `AGT_Agent1` 還是 `AGT_Agent3`？原因與對帳口徑是什麼？
2. 是否需要把 `GR_Currency` 放入 key？若不放，金額如何正規化？
3. 押注應使用 `GR_Bets`、`GR_ValidBets`、`MathBet` 還是 `MathTotalBet`？不得只看欄名猜語意。
4. 派彩使用 `GR_Win` 是否包含所有需要的 Jackpot／Feature 結果？
5. 購買玩法要納入統計、控制或只記帳？`GR_BetType` 1、3 如何處理？
6. `IsTestRNG`、Interrupt、零押注、非法 version 遇到時如何處理？
7. 需要逐轉順序時，排序依據是什麼？Kafka 只保證 partition 內順序；Distributed 查詢不保證全域順序。
8. 需要多長歷史窗？原始 `gr_slotgame` 目前保留 6 個月。
9. 需要 exactly-once、at-least-once，還是允許極小重複誤差？
10. 是事後分析／Shadow，還是要進入每轉即時決策 hot path？兩者架構不同。

## 10. 建議 AI 的輸出格式

針對新設計，AI 應固定分四段回答：

1. **已驗證現況**：引用本文件中的既有欄位、資料流與粒度。
2. **假設**：明列尚未由程式碼、DB 或業務確認的前提。
3. **設計決策**：說明選哪些欄位、key、時間窗及原因。
4. **驗證計畫**：提供可重播資料、邊界案例、A/B 或對帳方式。

不得只因為欄位存在就判定它適合作為演算法輸入；必須先確認語意、單位、粒度與更新順序。

## 11. 可重跑的唯讀查詢

### 最新資料時間

```sql
SELECT
    min(GR_EndTime) AS earliest_time,
    max(GR_EndTime) AS latest_time,
    count() AS row_count
FROM icrown.gr_slotgame_all
WHERE GR_EndTime >= now64(6, 'Etc/GMT+4') - INTERVAL 1 DAY;
```

### 版本與 BetType 分布

```sql
SELECT
    GameServer_Version,
    GR_BetType,
    count() AS row_count
FROM icrown.gr_slotgame_all
WHERE GR_EndTime >= now64(6, 'Etc/GMT+4') - INTERVAL 1 DAY
GROUP BY
    GameServer_Version,
    GR_BetType
ORDER BY row_count DESC;
```

### 資料品質檢查

```sql
SELECT
    count() AS total_rows,
    countIf(GR_Bets = 0) AS zero_bet_rows,
    countIf(GR_Bets < 0) AS negative_bet_rows,
    countIf(GR_Win < 0) AS negative_win_rows,
    countIf(IsTestRNG = 1) AS test_rng_rows,
    countIf(GR_FlagInterrupt = 1) AS interrupted_rows,
    countIf(NOT match(GameServer_Version, '^0[0-9]{3}$')) AS invalid_version_rows
FROM icrown.gr_slotgame_all
WHERE GR_EndTime >= now64(6, 'Etc/GMT+4') - INTERVAL 1 DAY;
```
