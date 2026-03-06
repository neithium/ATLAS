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
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS atlas.telemetry_refined
(
    report_id String,
    device_id String,
    application_customer_id String,
    platform_customer_id String,
    status UInt8,
    report_type String,
    metric_value Float64,
    model String,
    location_state String,
    location_country String,
    processor_vendor String,
    server_generation String,
    location_id String,
    location_name String,
    location_city String,
    server_name String,
    metric_id String,
    avg_metric_value Float64,
    max_metric_value Float64,
    min_metric_value Float64,
    metric_time DateTime64(3),
    amb_temp Float64,
    insertion_time DateTime64(3) DEFAULT now64(3),
    co2_factor Float64,
    energy_cost_factor Float64,
    location_date Date
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(metric_time)
ORDER BY (platform_customer_id, application_customer_id, device_id, metric_time)
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Hourly Aggregation Materialized View
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
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (platform_customer_id, application_customer_id, device_id, hour);

-- Materialized view to populate hourly aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS atlas.telemetry_hourly_mv
TO atlas.telemetry_hourly
AS SELECT
    platform_customer_id,
    application_customer_id,
    device_id,
    toStartOfHour(metric_time) AS hour,
    avg(metric_value) AS avg_metric_value,
    max(metric_value) AS max_metric_value,
    min(metric_value) AS min_metric_value,
    count() AS record_count,
    avg(amb_temp) AS avg_amb_temp
FROM atlas.telemetry_refined
GROUP BY platform_customer_id, application_customer_id, device_id, hour;

-- -----------------------------------------------------------------------------
-- Daily Aggregation Materialized View (Validation against Spark)
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
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (platform_customer_id, application_customer_id, device_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS atlas.telemetry_daily_mv
TO atlas.telemetry_daily
AS SELECT
    platform_customer_id,
    application_customer_id,
    device_id,
    toDate(metric_time) AS day,
    avg(metric_value) AS avg_metric_value,
    max(metric_value) AS max_metric_value,
    min(metric_value) AS min_metric_value,
    count() AS record_count,
    avg(amb_temp) AS avg_amb_temp
FROM atlas.telemetry_refined
GROUP BY platform_customer_id, application_customer_id, device_id, day;
