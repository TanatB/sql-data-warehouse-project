-- ============================================================================
-- Bronze Layer DDL - Raw Data Landing Zone
-- ============================================================================
-- Purpose: Raw data ingestion layer with minimal transformation
-- Schema: bronze
-- Data Format: Raw data as received from source systems
-- Retention: Full historical data with partitioning for performance
-- ============================================================================

-- Connect to warehouse database
\c warehouse_db

-- Set search path
SET search_path TO bronze, public;

-- ============================================================================
-- SECTION 1: DROP EXISTING TABLES (Use with caution!)
-- ============================================================================
-- Uncomment to recreate tables from scratch
-- DROP TABLE IF EXISTS bronze.raw_weather_data CASCADE;
-- DROP TABLE IF EXISTS bronze.raw_api_logs CASCADE;
-- DROP TABLE IF EXISTS bronze.raw_file_ingestion_log CASCADE;

-- ============================================================================
-- SECTION 2: CREATE RAW WEATHER DATA TABLE (Main Bronze Table)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_weather_data (
    -- Primary key and technical columns
    raw_id BIGSERIAL,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    source_system VARCHAR(100) NOT NULL, -- 'OpenWeatherAPI', 'WeatherStack', etc.

    -- API response metadata
    api_response_id VARCHAR(255),
    api_call_timestamp TIMESTAMP,
    http_status_code INTEGER,

    -- Location data (as received from API)
    location_id VARCHAR(100),
    location_name VARCHAR(255),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    country_code VARCHAR(10),
    timezone VARCHAR(100),
    timezone_offset INTEGER, -- seconds from UTC

    -- Weather observation data
    observation_timestamp TIMESTAMP,
    observation_date DATE,

    -- Temperature data (Kelvin as received from most APIs)
    temperature DECIMAL(6, 2),
    temperature_feels_like DECIMAL(6, 2),
    temperature_min DECIMAL(6, 2),
    temperature_max DECIMAL(6, 2),

    -- Atmospheric data
    pressure INTEGER, -- hPa
    humidity INTEGER, -- percentage
    visibility INTEGER, -- meters

    -- Wind data
    wind_speed DECIMAL(6, 2), -- meter/sec
    wind_direction INTEGER, -- degrees
    wind_gust DECIMAL(6, 2), -- meter/sec

    -- Precipitation data
    rain_1h DECIMAL(6, 2), -- mm
    rain_3h DECIMAL(6, 2), -- mm
    snow_1h DECIMAL(6, 2), -- mm
    snow_3h DECIMAL(6, 2), -- mm

    -- Cloud and weather condition
    cloudiness INTEGER, -- percentage
    weather_condition_id INTEGER,
    weather_condition_main VARCHAR(100),
    weather_condition_description VARCHAR(255),
    weather_condition_icon VARCHAR(20),

    -- Sun and moon data
    sunrise_timestamp TIMESTAMP,
    sunset_timestamp TIMESTAMP,

    -- Raw JSON payload (for debugging and reprocessing)
    raw_json_payload JSONB,

    -- Data quality flags
    is_complete BOOLEAN DEFAULT FALSE,
    has_errors BOOLEAN DEFAULT FALSE,
    error_message TEXT,

    -- Audit columns
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    updated_at TIMESTAMP,

    -- Partition key (for time-based partitioning)
    PRIMARY KEY (raw_id, ingestion_date)
) PARTITION BY RANGE (ingestion_date);

-- Create indexes on the parent table
CREATE INDEX IF NOT EXISTS idx_bronze_raw_weather_source
    ON bronze.raw_weather_data(source_system);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_weather_location
    ON bronze.raw_weather_data(location_id, observation_timestamp);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_weather_obs_time
    ON bronze.raw_weather_data(observation_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_weather_ingestion
    ON bronze.raw_weather_data(ingestion_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_weather_json_gin
    ON bronze.raw_weather_data USING GIN (raw_json_payload);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_weather_quality
    ON bronze.raw_weather_data(has_errors, is_complete);

COMMENT ON TABLE bronze.raw_weather_data IS 'Raw weather data ingested from external APIs - partitioned by ingestion_date';

-- ============================================================================
-- SECTION 3: CREATE PARTITIONS FOR RAW WEATHER DATA
-- ============================================================================

-- Create partitions for the past month, current month, and next month
-- This should be automated via a cron job or Airflow DAG

-- Previous month partition
CREATE TABLE IF NOT EXISTS bronze.raw_weather_data_2025_09
    PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

-- Current month partition
CREATE TABLE IF NOT EXISTS bronze.raw_weather_data_2025_10
    PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

-- Next month partition
CREATE TABLE IF NOT EXISTS bronze.raw_weather_data_2025_11
    PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

-- Future month partition
CREATE TABLE IF NOT EXISTS bronze.raw_weather_data_2025_12
    PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- Default partition for any dates not covered
CREATE TABLE IF NOT EXISTS bronze.raw_weather_data_default
    PARTITION OF bronze.raw_weather_data
    DEFAULT;

-- ============================================================================
-- SECTION 4: CREATE API LOGS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_api_logs (
    log_id BIGSERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- API call details
    api_endpoint VARCHAR(500) NOT NULL,
    http_method VARCHAR(10) NOT NULL,
    request_headers JSONB,
    request_params JSONB,
    request_body JSONB,

    -- Response details
    response_status_code INTEGER,
    response_headers JSONB,
    response_body JSONB,
    response_time_ms INTEGER, -- milliseconds

    -- Error tracking
    is_successful BOOLEAN DEFAULT TRUE,
    error_type VARCHAR(100),
    error_message TEXT,

    -- Rate limiting
    api_calls_remaining INTEGER,
    rate_limit_reset_timestamp TIMESTAMP,

    -- Audit
    called_by VARCHAR(100) DEFAULT CURRENT_USER,
    job_name VARCHAR(255),
    execution_id UUID
);

CREATE INDEX IF NOT EXISTS idx_bronze_api_logs_timestamp
    ON bronze.raw_api_logs(log_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_api_logs_endpoint
    ON bronze.raw_api_logs(api_endpoint);

CREATE INDEX IF NOT EXISTS idx_bronze_api_logs_status
    ON bronze.raw_api_logs(is_successful, response_status_code);

CREATE INDEX IF NOT EXISTS idx_bronze_api_logs_execution
    ON bronze.raw_api_logs(execution_id);

COMMENT ON TABLE bronze.raw_api_logs IS 'Logs all API calls made to external weather data sources';

-- ============================================================================
-- SECTION 5: CREATE FILE INGESTION LOG TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_file_ingestion_log (
    ingestion_log_id BIGSERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- File details
    file_name VARCHAR(500) NOT NULL,
    file_path VARCHAR(1000),
    file_size_bytes BIGINT,
    file_format VARCHAR(50), -- 'CSV', 'JSON', 'PARQUET', 'AVRO'
    file_hash VARCHAR(64), -- SHA256 hash for deduplication

    -- Source details
    source_system VARCHAR(100),
    source_location VARCHAR(500), -- S3 bucket, FTP server, etc.

    -- Processing details
    records_total INTEGER,
    records_processed INTEGER,
    records_failed INTEGER,
    processing_start_time TIMESTAMP,
    processing_end_time TIMESTAMP,
    processing_duration_seconds INTEGER,

    -- Status
    ingestion_status VARCHAR(50) NOT NULL, -- 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
    error_message TEXT,

    -- Audit
    ingested_by VARCHAR(100) DEFAULT CURRENT_USER,
    job_name VARCHAR(255),
    execution_id UUID
);

CREATE INDEX IF NOT EXISTS idx_bronze_file_ingestion_timestamp
    ON bronze.raw_file_ingestion_log(ingestion_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_file_ingestion_status
    ON bronze.raw_file_ingestion_log(ingestion_status);

CREATE INDEX IF NOT EXISTS idx_bronze_file_ingestion_hash
    ON bronze.raw_file_ingestion_log(file_hash);

CREATE INDEX IF NOT EXISTS idx_bronze_file_ingestion_execution
    ON bronze.raw_file_ingestion_log(execution_id);

COMMENT ON TABLE bronze.raw_file_ingestion_log IS 'Tracks file ingestion from various sources into Bronze layer';

-- ============================================================================
-- SECTION 6: CREATE DATA VALIDATION TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_data_validation (
    validation_id BIGSERIAL PRIMARY KEY,
    validation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Reference to raw data
    table_name VARCHAR(255) NOT NULL,
    record_id BIGINT,
    record_identifier VARCHAR(500), -- Business key or natural key

    -- Validation details
    validation_rule VARCHAR(255) NOT NULL,
    validation_type VARCHAR(100) NOT NULL, -- 'NULL_CHECK', 'FORMAT_CHECK', 'RANGE_CHECK', etc.
    validation_passed BOOLEAN NOT NULL,

    -- Failure details
    field_name VARCHAR(255),
    field_value TEXT,
    expected_value TEXT,
    failure_reason TEXT,

    -- Audit
    validated_by VARCHAR(100) DEFAULT CURRENT_USER,
    execution_id UUID
);

CREATE INDEX IF NOT EXISTS idx_bronze_validation_timestamp
    ON bronze.raw_data_validation(validation_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_validation_table
    ON bronze.raw_data_validation(table_name, validation_passed);

CREATE INDEX IF NOT EXISTS idx_bronze_validation_passed
    ON bronze.raw_data_validation(validation_passed);

COMMENT ON TABLE bronze.raw_data_validation IS 'Stores validation results for raw data quality checks';

-- ============================================================================
-- SECTION 7: CREATE VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View for latest weather data by location
CREATE OR REPLACE VIEW bronze.vw_latest_weather_by_location AS
SELECT DISTINCT ON (location_id)
    raw_id,
    location_id,
    location_name,
    latitude,
    longitude,
    observation_timestamp,
    temperature,
    humidity,
    wind_speed,
    weather_condition_description,
    ingestion_timestamp
FROM bronze.raw_weather_data
WHERE has_errors = FALSE
ORDER BY location_id, observation_timestamp DESC;

COMMENT ON VIEW bronze.vw_latest_weather_by_location IS 'Latest weather observation for each location';

-- View for data quality summary
CREATE OR REPLACE VIEW bronze.vw_data_quality_summary AS
SELECT
    ingestion_date,
    source_system,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE is_complete = TRUE) as complete_records,
    COUNT(*) FILTER (WHERE has_errors = TRUE) as error_records,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_complete = TRUE) / COUNT(*), 2) as completion_rate,
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as last_ingestion
FROM bronze.raw_weather_data
GROUP BY ingestion_date, source_system
ORDER BY ingestion_date DESC, source_system;

COMMENT ON VIEW bronze.vw_data_quality_summary IS 'Daily data quality metrics by source system';

-- ============================================================================
-- SECTION 8: CREATE FUNCTIONS FOR DATA VALIDATION
-- ============================================================================

-- Function to validate required fields in raw weather data
CREATE OR REPLACE FUNCTION bronze.validate_raw_weather_data(
    p_raw_id BIGINT
) RETURNS BOOLEAN AS $$
DECLARE
    v_record RECORD;
    v_is_valid BOOLEAN := TRUE;
BEGIN
    -- Get the record
    SELECT * INTO v_record
    FROM bronze.raw_weather_data
    WHERE raw_id = p_raw_id;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    -- Check required fields
    IF v_record.location_id IS NULL OR
       v_record.observation_timestamp IS NULL OR
       v_record.temperature IS NULL THEN
        v_is_valid := FALSE;
    END IF;

    -- Update the record
    UPDATE bronze.raw_weather_data
    SET is_complete = v_is_valid,
        has_errors = NOT v_is_valid,
        updated_at = CURRENT_TIMESTAMP
    WHERE raw_id = p_raw_id;

    RETURN v_is_valid;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION bronze.validate_raw_weather_data IS 'Validates required fields in raw weather data record';

-- ============================================================================
-- SECTION 9: CREATE TRIGGER FOR AUTOMATIC VALIDATION
-- ============================================================================

-- Trigger function to auto-validate on insert
CREATE OR REPLACE FUNCTION bronze.trg_validate_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    -- Set is_complete flag based on required fields
    NEW.is_complete := (
        NEW.location_id IS NOT NULL AND
        NEW.observation_timestamp IS NOT NULL AND
        NEW.temperature IS NOT NULL
    );

    -- Set has_errors flag
    NEW.has_errors := NOT NEW.is_complete;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS trg_validate_raw_weather ON bronze.raw_weather_data;
CREATE TRIGGER trg_validate_raw_weather
    BEFORE INSERT OR UPDATE ON bronze.raw_weather_data
    FOR EACH ROW
    EXECUTE FUNCTION bronze.trg_validate_on_insert();

-- ============================================================================
-- SECTION 10: GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to roles
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO dw_developer, airflow_service;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO dw_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO dw_developer, airflow_service;
GRANT SELECT ON ALL VIEWS IN SCHEMA bronze TO dw_developer, dw_analyst;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bronze TO dw_developer, airflow_service;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Bronze Layer DDL completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - bronze.raw_weather_data (partitioned)';
    RAISE NOTICE '  - bronze.raw_api_logs';
    RAISE NOTICE '  - bronze.raw_file_ingestion_log';
    RAISE NOTICE '  - bronze.raw_data_validation';
    RAISE NOTICE '';
    RAISE NOTICE 'Partitions created for: 2025-09, 2025-10, 2025-11, 2025-12';
    RAISE NOTICE 'Views created: vw_latest_weather_by_location, vw_data_quality_summary';
    RAISE NOTICE 'Functions created: validate_raw_weather_data';
    RAISE NOTICE '';
    RAISE NOTICE 'Next step: Run ddl_silver.sql to create Silver layer tables';
    RAISE NOTICE '============================================================================';
END $$;
