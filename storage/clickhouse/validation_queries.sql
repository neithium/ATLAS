-- =============================================================================
-- ATLAS ClickHouse — Validation Queries
-- =============================================================================
-- Run these after atlas-loader has completed to verify correctness of the
-- data load and materialized views.
--
-- Expected baseline (from generate_data.py + delta_merge_pipeline.py):
--   - telemetry_refined : 2304 rows  (2016 baseline + 288 new from overlap)
--   - telemetry_daily   : 8 day-rows (Feb 26 – Mar 05, 2026)
--   - telemetry_hourly  : ~192 hour-rows (8 days × 24 hours)
--   - Device SRV-101, Customer PLAT-12345 / APP-67890
-- =============================================================================


-- ============================================================
-- 1. ROW COUNT CHECK
-- ============================================================
SELECT
    'telemetry_refined' AS table_name,
    count()             AS row_count
FROM atlas.telemetry_refined;


-- ============================================================
-- 2. DUPLICATE CHECK
-- ============================================================
-- Should return 0 rows.
SELECT
    device_id,
    metric_time,
    count() AS dup_count
FROM atlas.telemetry_refined
GROUP BY device_id, metric_time
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20;


-- ============================================================
-- 3. NULL AUDIT — Critical columns must not be NULL / empty
-- ============================================================
SELECT
    countIf(device_id = '')                     AS empty_device_id,
    countIf(metric_time IS NULL)                AS null_metric_time,
    countIf(MetricValue = 0 AND amb_temp = 0)   AS suspicious_zero_rows,
    countIf(platform_customer_id = '')          AS empty_platform_cid,
    countIf(application_customer_id = '')       AS empty_app_cid
FROM atlas.telemetry_refined;


-- ============================================================
-- 4. MV POPULATION CHECK
-- ============================================================
-- Both MVs should have rows after data load.
SELECT
    (SELECT count() FROM atlas.telemetry_hourly) AS hourly_rows,
    (SELECT count() FROM atlas.telemetry_daily)  AS daily_rows;


-- ============================================================
-- 5. DAILY MV — Contents
-- ============================================================
SELECT
    platform_customer_id,
    application_customer_id,
    device_id,
    day,
    avg_metric_value,
    max_metric_value,
    min_metric_value,
    record_count,
    avg_amb_temp,
    total_energy_cost,
    total_co2
FROM atlas.telemetry_daily
ORDER BY day;


-- ============================================================
-- 6. DAILY MV CORRECTNESS — Compare MV output vs manual GROUP BY
-- ============================================================
-- Should return 0 rows (no mismatches).
SELECT
    'mismatch' AS check_type,
    m.day,
    m.device_id,
    m.avg_metric_value   AS mv_avg,
    g.avg_metric_value   AS manual_avg,
    m.max_metric_value   AS mv_max,
    g.max_metric_value   AS manual_max,
    m.min_metric_value   AS mv_min,
    g.min_metric_value   AS manual_min,
    m.record_count       AS mv_count,
    g.record_count       AS manual_count
FROM atlas.telemetry_daily AS m
FULL OUTER JOIN (
    SELECT
        platform_customer_id,
        application_customer_id,
        device_id,
        toDate(metric_time) AS day,
        report_type,
        location_country,
        avg(MetricValue)    AS avg_metric_value,
        max(MetricValue)    AS max_metric_value,
        min(MetricValue)    AS min_metric_value,
        count()             AS record_count
    FROM atlas.telemetry_refined
    GROUP BY platform_customer_id, application_customer_id, device_id,
             day, report_type, location_country
) AS g
ON  m.platform_customer_id    = g.platform_customer_id
AND m.application_customer_id = g.application_customer_id
AND m.device_id               = g.device_id
AND m.day                     = g.day
WHERE
    abs(m.avg_metric_value - g.avg_metric_value) > 0.001
    OR m.max_metric_value != g.max_metric_value
    OR m.min_metric_value != g.min_metric_value
    OR m.record_count     != g.record_count;


-- ============================================================
-- 7. HOURLY → DAILY CROSS-CHECK
-- ============================================================
-- Re-aggregate the hourly MV into daily granularity and compare
-- record_count with the daily MV.
-- NOTE: avg-of-avg != avg-of-raw, so we only compare record_count,
--       max, and min — not averages.
SELECT
    'hourly_vs_daily' AS check_type,
    d.day,
    d.device_id,
    d.record_count        AS daily_count,
    h.record_count        AS hourly_reagg_count
FROM atlas.telemetry_daily AS d
FULL OUTER JOIN (
    SELECT
        platform_customer_id,
        application_customer_id,
        device_id,
        toDate(hour) AS day,
        report_type,
        location_country,
        max(max_metric_value)          AS max_metric_value,
        min(min_metric_value)          AS min_metric_value,
        sum(record_count)              AS record_count
    FROM atlas.telemetry_hourly
    GROUP BY platform_customer_id, application_customer_id, device_id,
             day, report_type, location_country
) AS h
ON  d.platform_customer_id    = h.platform_customer_id
AND d.application_customer_id = h.application_customer_id
AND d.device_id               = h.device_id
AND d.day                     = h.day
WHERE
    d.record_count != h.record_count
    OR d.max_metric_value != h.max_metric_value
    OR d.min_metric_value != h.min_metric_value;


-- ============================================================
-- 8. HOURLY MV — Sample output (first 24 hours)
-- ============================================================
SELECT *
FROM atlas.telemetry_hourly
ORDER BY hour
LIMIT 24;


-- ============================================================
-- 9. TIME RANGE SANITY
-- ============================================================
SELECT
    min(metric_time) AS earliest,
    max(metric_time) AS latest,
    dateDiff('day', min(metric_time), max(metric_time)) AS span_days,
    count(DISTINCT toDate(metric_time)) AS distinct_days,
    count(DISTINCT device_id) AS distinct_devices
FROM atlas.telemetry_refined;
