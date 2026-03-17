-- =============================================================================
-- ATLAS ClickHouse Initialization Script
-- =============================================================================
-- This script runs automatically when the ClickHouse container starts
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create ATLAS database
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS atlas;

-- -----------------------------------------------------------------------------
-- Raw Telemetry Table (from Kafka Engine - Fast Path)
-- -----------------------------------------------------------------------------
-- TODO: Varna - Configure Kafka Engine for live streaming
-- CREATE TABLE IF NOT EXISTS atlas.raw_telemetry_kafka
-- (
--     device_id String,
--     platform_customer_id String,
--     application_customer_id String,
--     metric_time DateTime64(3),
--     metric_value Float64,
--     amb_temp Float64
-- ) ENGINE = Kafka()
-- SETTINGS
--     kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
--     kafka_topic_list = 'raw-server-metrics',
--     kafka_group_name = 'clickhouse-consumer',
--     kafka_format = 'JSONEachRow';

-- -----------------------------------------------------------------------------
-- Deduplicated Telemetry Table (from Delta Lake - Batch Path)
-- Aligned 1:1 with schema/output_schema.py (36 columns)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS atlas.telemetry_refined
(
    -- Identity & metadata
    report_id String,
    device_id String,
    application_customer_id String,
    platform_customer_id String,
    status UInt8,                              -- Boolean in Spark → UInt8 (0/1)
    report_type String,
    error_reason Nullable(String),
    MetricValue Float64,                       -- Raw per-reading metric
    model String,
    tags String,
    -- Location
    location_state String,
    location_country String,
    processor_vendor String,
    server_generation String,
    location_id String,
    location_name String,
    location_city String,
    -- Hardware
    server_name String,
    metric_id String,
    cpu_inventory String,                      -- Serialised JSON string from upstream
    memory_inventory String,                   -- Serialised JSON string from upstream
    pcie_devices_count UInt32,
    socket_count UInt32,
    -- Metric aggregates (pre-computed by Spark)
    avg_metric_value Float64,
    max_metric_value Float64,
    min_metric_value Float64,
    -- Time columns
    metric_time DateTime64(3),                 -- Parsed from ISO-8601 string by loader
    datetime Float64,                          -- Epoch seconds (upstream)
    timeRangeEnd Float64,                      -- Epoch seconds end of window
    amb_temp Float64,
    Insertiontime Float64,                     -- Upstream insertion epoch
    -- ClickHouse-side insertion timestamp (auto-populated, not from upstream)
    insertion_time DateTime64(3) DEFAULT now64(3),
    -- Cost & environmental factors
    co2_factor Float64,
    energy_cost_factor Float64,
    -- Date columns
    max_metric_time String,
    location_date String,                      
    inventory_date String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(metric_time)
ORDER BY (platform_customer_id, application_customer_id, device_id, metric_time)
TTL metric_time + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Hourly Aggregation Materialized View
-- -----------------------------------------------------------------------------
-- MergeTree (not SummingMergeTree) because SummingMergeTree sums ALL numeric
-- columns on background merges, which corrupts avg/min/max aggregates.
-- The loader inserts once per cycle so duplicate rollup rows are not expected.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS atlas.telemetry_hourly
(
    platform_customer_id String,
    application_customer_id String,
    device_id String,
    hour DateTime,
    avg_metric_value Float64,
    max_metric_value Float64,
    min_metric_value Float64,
    record_count UInt64,
    avg_amb_temp Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (platform_customer_id, application_customer_id, device_id, hour);

-- Materialized view: aggregates raw MetricValue per hour per device
CREATE MATERIALIZED VIEW IF NOT EXISTS atlas.telemetry_hourly_mv
TO atlas.telemetry_hourly
AS SELECT
    platform_customer_id,
    application_customer_id,
    device_id,
    toStartOfHour(metric_time) AS hour,
    avg(MetricValue) AS avg_metric_value,
    max(MetricValue) AS max_metric_value,
    min(MetricValue) AS min_metric_value,
    count() AS record_count,
    avg(amb_temp) AS avg_amb_temp
FROM atlas.telemetry_refined
GROUP BY platform_customer_id, application_customer_id, device_id, hour;

-- -----------------------------------------------------------------------------
-- Daily Aggregation Materialized View (Validation against Spark)
-- -----------------------------------------------------------------------------
-- MergeTree for same reason as hourly — protects avg/min/max from merge corruption.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS atlas.telemetry_daily
(
    platform_customer_id String,
    application_customer_id String,
    device_id String,
    day Date,
    avg_metric_value Float64,
    max_metric_value Float64,
    min_metric_value Float64,
    record_count UInt64,
    avg_amb_temp Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (platform_customer_id, application_customer_id, device_id, day)
TTL day + INTERVAL 3 YEAR DELETE;

-- Materialized view: 24-hour rollups from raw MetricValue
CREATE MATERIALIZED VIEW IF NOT EXISTS atlas.telemetry_daily_mv
TO atlas.telemetry_daily
AS SELECT
    platform_customer_id,
    application_customer_id,
    device_id,
    toDate(metric_time) AS day,
    avg(MetricValue) AS avg_metric_value,
    max(MetricValue) AS max_metric_value,
    min(MetricValue) AS min_metric_value,
    count() AS record_count,
    avg(amb_temp) AS avg_amb_temp
FROM atlas.telemetry_refined
GROUP BY platform_customer_id, application_customer_id, device_id, day;
