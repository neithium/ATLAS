-- =============================================================================
-- ATLAS ClickHouse Validation Queries
-- =============================================================================
-- Run these against atlas.telemetry_refined to verify data integrity.
-- All checks should return 0 rows or expected values.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check 1: Row count summary
-- -----------------------------------------------------------------------------
SELECT
    'telemetry_refined' AS table_name, count() AS row_count
FROM atlas.telemetry_refined
UNION ALL
SELECT
    'telemetry_hourly', count()
FROM atlas.telemetry_hourly
UNION ALL
SELECT
    'telemetry_daily', count()
FROM atlas.telemetry_daily;

-- -----------------------------------------------------------------------------
-- Check 2: Duplicate detection (composite key uniqueness)
-- Uses full composite key: device_id + platform_customer_id +
-- application_customer_id + metric_time
-- Should return 0 rows if deduplication is working correctly.
-- -----------------------------------------------------------------------------
SELECT
    device_id,
    platform_customer_id,
    application_customer_id,
    metric_time,
    count() AS dup_count
FROM atlas.telemetry_refined
GROUP BY device_id, platform_customer_id, application_customer_id, metric_time
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- Check 3: NULL check on critical non-nullable columns
-- Should return 0 for all counts.
-- -----------------------------------------------------------------------------
SELECT
    countIf(device_id = '')               AS empty_device_id,
    countIf(platform_customer_id = '')    AS empty_pcid,
    countIf(application_customer_id = '') AS empty_acid,
    countIf(metric_time = toDateTime64('1970-01-01 00:00:00.000', 3)) AS epoch_metric_time
FROM atlas.telemetry_refined;

-- -----------------------------------------------------------------------------
-- Check 4: Hourly MV correctness — compare MV output against manual rollup
-- Differences indicate MV is out of sync.
-- Should return 0 rows.
-- -----------------------------------------------------------------------------
SELECT
    h.platform_customer_id,
    h.application_customer_id,
    h.device_id,
    h.hour,
    h.record_count AS mv_count,
    m.record_count AS manual_count
FROM atlas.telemetry_hourly AS h
FULL OUTER JOIN (
    SELECT
        platform_customer_id,
        application_customer_id,
        device_id,
        toStartOfHour(metric_time) AS hour,
        count() AS record_count
    FROM atlas.telemetry_refined
    GROUP BY platform_customer_id, application_customer_id, device_id, hour
) AS m
ON h.platform_customer_id = m.platform_customer_id
    AND h.application_customer_id = m.application_customer_id
    AND h.device_id = m.device_id
    AND h.hour = m.hour
WHERE h.record_count != m.record_count
    OR h.record_count IS NULL
    OR m.record_count IS NULL
LIMIT 20;

-- -----------------------------------------------------------------------------
-- Check 5: Daily MV correctness — compare MV output against manual rollup
-- Should return 0 rows.
-- -----------------------------------------------------------------------------
SELECT
    d.platform_customer_id,
    d.application_customer_id,
    d.device_id,
    d.day,
    d.record_count AS mv_count,
    m.record_count AS manual_count
FROM atlas.telemetry_daily AS d
FULL OUTER JOIN (
    SELECT
        platform_customer_id,
        application_customer_id,
        device_id,
        toDate(metric_time) AS day,
        count() AS record_count
    FROM atlas.telemetry_refined
    GROUP BY platform_customer_id, application_customer_id, device_id, day
) AS m
ON d.platform_customer_id = m.platform_customer_id
    AND d.application_customer_id = m.application_customer_id
    AND d.device_id = m.device_id
    AND d.day = m.day
WHERE d.record_count != m.record_count
    OR d.record_count IS NULL
    OR m.record_count IS NULL
LIMIT 20;

-- -----------------------------------------------------------------------------
-- Check 6: Data range summary
-- Quick overview of data freshness and coverage.
-- -----------------------------------------------------------------------------
SELECT
    count()                         AS total_rows,
    min(metric_time)                AS earliest_metric_time,
    max(metric_time)                AS latest_metric_time,
    uniqExact(device_id)            AS unique_devices,
    uniqExact(platform_customer_id) AS unique_platforms,
    uniqExact(report_type)          AS unique_report_types
FROM atlas.telemetry_refined;
