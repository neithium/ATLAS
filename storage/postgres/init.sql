-- =============================================================================
-- ATLAS PostgreSQL Initialization Script
-- =============================================================================
-- This script runs automatically when the PostgreSQL container starts
-- =============================================================================

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- Device Registry Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS device_registry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id VARCHAR(100) NOT NULL,
    platform_customer_id VARCHAR(100) NOT NULL,
    application_customer_id VARCHAR(100) NOT NULL,
    server_name VARCHAR(255),
    model VARCHAR(255),
    processor_vendor VARCHAR(100),
    server_generation VARCHAR(50),
    socket_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id, platform_customer_id, application_customer_id)
);

-- -----------------------------------------------------------------------------
-- Location Registry Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS location_registry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id VARCHAR(100) UNIQUE NOT NULL,
    location_name VARCHAR(255),
    location_city VARCHAR(100),
    location_state VARCHAR(100),
    location_country VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Pipeline Run Metadata Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id VARCHAR(100) UNIQUE NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    records_processed BIGINT DEFAULT 0,
    records_deduplicated BIGINT DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT
);

-- -----------------------------------------------------------------------------
-- Create indexes for common queries
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_device_platform ON device_registry(platform_customer_id);
CREATE INDEX IF NOT EXISTS idx_device_application ON device_registry(application_customer_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_started ON pipeline_runs(started_at);

-- -----------------------------------------------------------------------------
-- Data Load Watermarks Table (Tracks incremental loads into ClickHouse)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_load_watermarks (
    source VARCHAR(100) PRIMARY KEY,
    last_metric_time TIMESTAMP WITH TIME ZONE,
    last_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    rows_loaded BIGINT DEFAULT 0
);

-- -----------------------------------------------------------------------------
-- Grant permissions
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO atlas;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO atlas;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'ATLAS PostgreSQL initialization completed successfully!';
END $$;
