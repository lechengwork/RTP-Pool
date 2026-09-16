-- 目前尚未接資料來源時，event_rows = 0 是正常結果。
SELECT
    count() AS event_rows,
    min(GR_EndTime) AS earliest_time,
    max(GR_EndTime) AS latest_time
FROM icrown.rtp_basic_pool_event_log
FINAL;

SELECT
    AGT_Agent1,
    GM_GameCode,
    GameServer_Version,
    bet,
    win
FROM icrown.rtp_basic_pool_stats
ORDER BY bet DESC
LIMIT 20;
