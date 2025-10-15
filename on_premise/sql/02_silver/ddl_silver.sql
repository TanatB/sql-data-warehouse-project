-- ============================================================================
-- Silver Layer DDL - Cleaned and Conformed Data
-- ============================================================================
-- Purpose: Cleaned, validated, and standardized data layer
-- Schema: silver
-- Data Format: Cleaned data with business rules applied
-- Quality: High data quality with constraints and validation
-- ============================================================================

-- Connect to warehouse database
\c warehouse_db

-- Set search path
SET search_path TO silver, bronze, public;

-- ============================================================================
-- SECTION 1: DROP EXISTING TABLES (Use with caution!)
-- ============================================================================
-- Uncomment to recreate tables from scratch
-- DROP TABLE IF EXISTS silver.weather_observations CASCADE;
-- DROP TABLE IF EXISTS silver.locations CASCADE;
-- DROP TABLE IF EXISTS silver.weather_conditions CASCADE;

-- ============================================================================
-- SECTION 2: CREATE LOCATIONS DIMENSION (SCD Type 2)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.locations (
    location_key BIGSERIAL PRIMARY KEY,
    location_id VARCHAR(100) NOT NULL,  -- Natural key from source

    -- Location attributes
    location_name VARCHAR(255) NOT NULL,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(10, 7) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    timezone VARCHAR(100),
    timezone_offset_seconds INTEGER,

    -- Geographic enrichment
    region VARCHAR(255),
    climate_zone VARCHAR(100),
    elevation_meters INTEGER,

    -- Data quality
    data_quality_score DECIMAL(5, 2) CHECK (data_quality_score BETWEEN 0 AND 100),

    -- SCD Type 2 columns
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Audit columns
    source_system VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    updated_at TIMESTAMP,
    updated_by VARCHAR(100),

    -- Constraints
    CONSTRAINT chk_lat_range CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT chk_lon_range CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT chk_effective_dates CHECK (effective_from < effective_to)
);

-- Indexes for location table
CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_location_current
    ON silver.locations(location_id, is_current)
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_silver_location_natural_key
    ON silver.locations(location_id);

CREATE INDEX IF NOT EXISTS idx_silver_location_coords
    ON silver.locations(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_silver_location_country
    ON silver.locations(country_code);

CREATE INDEX IF NOT EXISTS idx_silver_location_effective
    ON silver.locations(effective_from, effective_to);

COMMENT ON TABLE silver.locations IS 'Location dimension with SCD Type 2 for tracking historical changes';

-- ============================================================================
-- SECTION 3: CREATE WEATHER CONDITIONS REFERENCE TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.weather_conditions (
    condition_key SERIAL PRIMARY KEY,
    condition_id INTEGER UNIQUE NOT NULL,  -- From API

    -- Condition details
    condition_main VARCHAR(100) NOT NULL,
    condition_description VARCHAR(255) NOT NULL,
    condition_icon VARCHAR(20),

    -- Categorization
    condition_category VARCHAR(100), -- 'CLEAR', 'CLOUDS', 'RAIN', 'SNOW', 'EXTREME'
    severity_level VARCHAR(50), -- 'LOW', 'MEDIUM', 'HIGH', 'SEVERE'

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX IF NOT EXISTS idx_silver_condition_category
    ON silver.weather_conditions(condition_category);

COMMENT ON TABLE silver.weather_conditions IS 'Weather condition reference data from API providers';

-- ============================================================================
-- SECTION 4: CREATE WEATHER OBSERVATIONS TABLE (Main Silver Table)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.weather_observations (
    observation_key BIGSERIAL,
    observation_id UUID DEFAULT uuid_generate_v4(),

    -- Foreign keys
    location_key BIGINT NOT NULL,
    condition_key INTEGER,

    -- Time dimensions
    observation_timestamp TIMESTAMP NOT NULL,
    observation_date DATE NOT NULL,
    observation_hour INTEGER NOT NULL CHECK (observation_hour BETWEEN 0 AND 23),
    observation_day_of_week INTEGER NOT NULL CHECK (observation_day_of_week BETWEEN 1 AND 7),
    observation_month INTEGER NOT NULL CHECK (observation_month BETWEEN 1 AND 12),
    observation_quarter INTEGER NOT NULL CHECK (observation_quarter BETWEEN 1 AND 4),
    observation_year INTEGER NOT NULL,

    -- Temperature (Converted to Celsius)
    temperature_celsius DECIMAL(6, 2) NOT NULL,
    temperature_fahrenheit DECIMAL(6, 2),
    feels_like_celsius DECIMAL(6, 2),
    feels_like_fahrenheit DECIMAL(6, 2),
    temperature_min_celsius DECIMAL(6, 2),
    temperature_max_celsius DECIMAL(6, 2),

    -- Temperature validation
    temperature_range_check BOOLEAN GENERATED ALWAYS AS (
        temperature_celsius BETWEEN -89.2 AND 56.7  -- Historical extreme temps
    ) STORED,

    -- Atmospheric data
    pressure_hpa INTEGER CHECK (pressure_hpa BETWEEN 870 AND 1085),  -- Realistic range
    humidity_percent INTEGER NOT NULL CHECK (humidity_percent BETWEEN 0 AND 100),
    visibility_meters INTEGER CHECK (visibility_meters >= 0),

    -- Wind data
    wind_speed_mps DECIMAL(6, 2) CHECK (wind_speed_mps >= 0),  -- meters per second
    wind_speed_kmh DECIMAL(6, 2),  -- kilometers per hour
    wind_direction_degrees INTEGER CHECK (wind_direction_degrees BETWEEN 0 AND 360),
    wind_direction_cardinal VARCHAR(5),  -- N, NE, E, SE, S, SW, W, NW
    wind_gust_mps DECIMAL(6, 2) CHECK (wind_gust_mps >= 0),

    -- Beaufort scale classification
    beaufort_scale INTEGER CHECK (beaufort_scale BETWEEN 0 AND 12),
    wind_description VARCHAR(100),

    -- Precipitation
    rain_1h_mm DECIMAL(6, 2) CHECK (rain_1h_mm >= 0),
    rain_3h_mm DECIMAL(6, 2) CHECK (rain_3h_mm >= 0),
    snow_1h_mm DECIMAL(6, 2) CHECK (snow_1h_mm >= 0),
    snow_3h_mm DECIMAL(6, 2) CHECK (snow_3h_mm >= 0),
    precipitation_total_mm DECIMAL(6, 2),

    -- Cloud coverage
    cloudiness_percent INTEGER CHECK (cloudiness_percent BETWEEN 0 AND 100),
    cloud_description VARCHAR(100),

    -- Sun data
    sunrise_time TIMESTAMP,
    sunset_time TIMESTAMP,
    daylight_duration_minutes INTEGER,

    -- Calculated fields
    heat_index DECIMAL(6, 2),  -- Feels like temperature accounting for humidity
    wind_chill DECIMAL(6, 2),  -- Apparent temperature from wind
    dew_point DECIMAL(6, 2),   -- Temperature at which air becomes saturated

    -- Weather indices
    uv_index DECIMAL(4, 2) CHECK (uv_index >= 0),
    air_quality_index INTEGER,

    -- Data lineage
    source_raw_id BIGINT,  -- Reference to bronze.raw_weather_data
    source_system VARCHAR(100) NOT NULL,

    -- Data quality
    quality_score DECIMAL(5, 2) CHECK (quality_score BETWEEN 0 AND 100),
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_reason TEXT,

    -- Audit columns
    ingestion_timestamp TIMESTAMP NOT NULL,
    processed_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    updated_at TIMESTAMP,

    -- Primary key (observation_key, observation_date for partitioning)
    PRIMARY KEY (observation_key, observation_date),

    -- Foreign key constraints
    CONSTRAINT fk_location FOREIGN KEY (location_key)
        REFERENCES silver.locations(location_key),
    CONSTRAINT fk_condition FOREIGN KEY (condition_key)
        REFERENCES silver.weather_conditions(condition_key)
) PARTITION BY RANGE (observation_date);

-- ============================================================================
-- SECTION 5: CREATE PARTITIONS FOR WEATHER OBSERVATIONS
-- ============================================================================

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS silver.weather_observations_2025_09
    PARTITION OF silver.weather_observations
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE IF NOT EXISTS silver.weather_observations_2025_10
    PARTITION OF silver.weather_observations
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE IF NOT EXISTS silver.weather_observations_2025_11
    PARTITION OF silver.weather_observations
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE IF NOT EXISTS silver.weather_observations_2025_12
    PARTITION OF silver.weather_observations
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS silver.weather_observations_default
    PARTITION OF silver.weather_observations
    DEFAULT;

-- ============================================================================
-- SECTION 6: CREATE INDEXES FOR WEATHER OBSERVATIONS
-- ============================================================================

-- Time-based indexes (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_silver_obs_timestamp
    ON silver.weather_observations(observation_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_silver_obs_date_location
    ON silver.weather_observations(observation_date, location_key);

CREATE INDEX IF NOT EXISTS idx_silver_obs_hour_location
    ON silver.weather_observations(observation_hour, location_key);

-- Location-based queries
CREATE INDEX IF NOT EXISTS idx_silver_obs_location
    ON silver.weather_observations(location_key, observation_timestamp DESC);

-- Condition-based queries
CREATE INDEX IF NOT EXISTS idx_silver_obs_condition
    ON silver.weather_observations(condition_key);

-- Temperature range queries
CREATE INDEX IF NOT EXISTS idx_silver_obs_temp
    ON silver.weather_observations(temperature_celsius);

-- Data quality queries
CREATE INDEX IF NOT EXISTS idx_silver_obs_quality
    ON silver.weather_observations(quality_score, is_anomaly);

-- Source tracking
CREATE INDEX IF NOT EXISTS idx_silver_obs_source
    ON silver.weather_observations(source_system, source_raw_id);

COMMENT ON TABLE silver.weather_observations IS 'Cleaned and validated weather observations with business rules applied';

-- ============================================================================
-- SECTION 7: CREATE DATA QUALITY TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.data_anomalies (
    anomaly_id BIGSERIAL PRIMARY KEY,
    detected_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Reference to observation
    observation_key BIGINT,
    location_key BIGINT,
    observation_timestamp TIMESTAMP NOT NULL,

    -- Anomaly details
    anomaly_type VARCHAR(100) NOT NULL, -- 'OUTLIER', 'MISSING_DATA', 'DUPLICATE', 'INVALID_VALUE'
    field_name VARCHAR(255),
    detected_value TEXT,
    expected_range TEXT,

    -- Statistical measures
    z_score DECIMAL(10, 4),
    confidence_level DECIMAL(5, 2),

    -- Resolution
    is_resolved BOOLEAN DEFAULT FALSE,
    resolution_action VARCHAR(255),
    resolved_by VARCHAR(100),
    resolved_at TIMESTAMP,

    -- Audit
    detected_by VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX IF NOT EXISTS idx_silver_anomalies_timestamp
    ON silver.data_anomalies(detected_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_silver_anomalies_type
    ON silver.data_anomalies(anomaly_type, is_resolved);

COMMENT ON TABLE silver.data_anomalies IS 'Tracks detected data anomalies in weather observations';

-- ============================================================================
-- SECTION 8: CREATE TRANSFORMATION VIEWS
-- ============================================================================

-- View for current weather by location
CREATE OR REPLACE VIEW silver.vw_current_weather AS
SELECT
    l.location_id,
    l.location_name,
    l.latitude,
    l.longitude,
    l.country_code,
    wo.observation_timestamp,
    wo.temperature_celsius,
    wo.temperature_fahrenheit,
    wo.feels_like_celsius,
    wo.humidity_percent,
    wo.pressure_hpa,
    wo.wind_speed_kmh,
    wo.wind_direction_cardinal,
    wc.condition_description,
    wo.cloudiness_percent,
    wo.quality_score
FROM silver.weather_observations wo
JOIN silver.locations l ON wo.location_key = l.location_key
LEFT JOIN silver.weather_conditions wc ON wo.condition_key = wc.condition_key
WHERE l.is_current = TRUE
    AND wo.observation_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    AND wo.quality_score >= 70
ORDER BY wo.observation_timestamp DESC;

COMMENT ON VIEW silver.vw_current_weather IS 'Most recent weather observations for each location';

-- View for daily weather summary
CREATE OR REPLACE VIEW silver.vw_daily_weather_summary AS
SELECT
    observation_date,
    location_key,
    COUNT(*) as observation_count,
    AVG(temperature_celsius) as avg_temperature_celsius,
    MIN(temperature_celsius) as min_temperature_celsius,
    MAX(temperature_celsius) as max_temperature_celsius,
    AVG(humidity_percent) as avg_humidity_percent,
    AVG(wind_speed_kmh) as avg_wind_speed_kmh,
    MAX(wind_speed_kmh) as max_wind_speed_kmh,
    SUM(COALESCE(rain_1h_mm, 0)) as total_rainfall_mm,
    AVG(quality_score) as avg_quality_score,
    COUNT(*) FILTER (WHERE is_anomaly = TRUE) as anomaly_count
FROM silver.weather_observations
GROUP BY observation_date, location_key
ORDER BY observation_date DESC, location_key;

COMMENT ON VIEW silver.vw_daily_weather_summary IS 'Daily aggregated weather statistics by location';

-- ============================================================================
-- SECTION 9: CREATE TRANSFORMATION FUNCTIONS
-- ============================================================================

-- Function to convert Kelvin to Celsius
CREATE OR REPLACE FUNCTION silver.kelvin_to_celsius(kelvin DECIMAL)
RETURNS DECIMAL AS $$
BEGIN
    RETURN ROUND(kelvin - 273.15, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to convert Celsius to Fahrenheit
CREATE OR REPLACE FUNCTION silver.celsius_to_fahrenheit(celsius DECIMAL)
RETURNS DECIMAL AS $$
BEGIN
    RETURN ROUND((celsius * 9.0 / 5.0) + 32, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to calculate wind direction cardinal
CREATE OR REPLACE FUNCTION silver.degrees_to_cardinal(degrees INTEGER)
RETURNS VARCHAR AS $$
BEGIN
    IF degrees IS NULL THEN
        RETURN NULL;
    END IF;

    CASE
        WHEN degrees >= 337.5 OR degrees < 22.5 THEN RETURN 'N';
        WHEN degrees >= 22.5 AND degrees < 67.5 THEN RETURN 'NE';
        WHEN degrees >= 67.5 AND degrees < 112.5 THEN RETURN 'E';
        WHEN degrees >= 112.5 AND degrees < 157.5 THEN RETURN 'SE';
        WHEN degrees >= 157.5 AND degrees < 202.5 THEN RETURN 'S';
        WHEN degrees >= 202.5 AND degrees < 247.5 THEN RETURN 'SW';
        WHEN degrees >= 247.5 AND degrees < 292.5 THEN RETURN 'W';
        WHEN degrees >= 292.5 AND degrees < 337.5 THEN RETURN 'NW';
        ELSE RETURN 'N';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to calculate Beaufort scale
CREATE OR REPLACE FUNCTION silver.wind_to_beaufort(wind_speed_mps DECIMAL)
RETURNS INTEGER AS $$
BEGIN
    IF wind_speed_mps IS NULL THEN
        RETURN NULL;
    END IF;

    CASE
        WHEN wind_speed_mps < 0.5 THEN RETURN 0;
        WHEN wind_speed_mps < 1.5 THEN RETURN 1;
        WHEN wind_speed_mps < 3.3 THEN RETURN 2;
        WHEN wind_speed_mps < 5.5 THEN RETURN 3;
        WHEN wind_speed_mps < 7.9 THEN RETURN 4;
        WHEN wind_speed_mps < 10.7 THEN RETURN 5;
        WHEN wind_speed_mps < 13.8 THEN RETURN 6;
        WHEN wind_speed_mps < 17.1 THEN RETURN 7;
        WHEN wind_speed_mps < 20.7 THEN RETURN 8;
        WHEN wind_speed_mps < 24.4 THEN RETURN 9;
        WHEN wind_speed_mps < 28.4 THEN RETURN 10;
        WHEN wind_speed_mps < 32.6 THEN RETURN 11;
        ELSE RETURN 12;
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to calculate dew point
CREATE OR REPLACE FUNCTION silver.calculate_dew_point(
    temperature_celsius DECIMAL,
    humidity_percent INTEGER
)
RETURNS DECIMAL AS $$
DECLARE
    a DECIMAL := 17.27;
    b DECIMAL := 237.7;
    alpha DECIMAL;
BEGIN
    IF temperature_celsius IS NULL OR humidity_percent IS NULL THEN
        RETURN NULL;
    END IF;

    alpha := ((a * temperature_celsius) / (b + temperature_celsius)) + LN(humidity_percent / 100.0);
    RETURN ROUND((b * alpha) / (a - alpha), 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to upsert location (SCD Type 2)
CREATE OR REPLACE FUNCTION silver.upsert_location(
    p_location_id VARCHAR,
    p_location_name VARCHAR,
    p_latitude DECIMAL,
    p_longitude DECIMAL,
    p_country_code VARCHAR,
    p_timezone VARCHAR,
    p_timezone_offset INTEGER,
    p_source_system VARCHAR
)
RETURNS BIGINT AS $$
DECLARE
    v_existing_key BIGINT;
    v_new_key BIGINT;
    v_has_changed BOOLEAN := FALSE;
BEGIN
    -- Check if current record exists
    SELECT location_key INTO v_existing_key
    FROM silver.locations
    WHERE location_id = p_location_id
        AND is_current = TRUE;

    IF v_existing_key IS NOT NULL THEN
        -- Check if attributes have changed
        SELECT EXISTS (
            SELECT 1
            FROM silver.locations
            WHERE location_key = v_existing_key
                AND (
                    location_name != p_location_name OR
                    latitude != p_latitude OR
                    longitude != p_longitude OR
                    country_code != p_country_code OR
                    COALESCE(timezone, '') != COALESCE(p_timezone, '') OR
                    COALESCE(timezone_offset_seconds, 0) != COALESCE(p_timezone_offset, 0)
                )
        ) INTO v_has_changed;

        IF v_has_changed THEN
            -- Close current record
            UPDATE silver.locations
            SET effective_to = CURRENT_TIMESTAMP,
                is_current = FALSE
            WHERE location_key = v_existing_key;

            -- Insert new record
            INSERT INTO silver.locations (
                location_id, location_name, latitude, longitude,
                country_code, timezone, timezone_offset_seconds,
                source_system, effective_from, is_current
            ) VALUES (
                p_location_id, p_location_name, p_latitude, p_longitude,
                p_country_code, p_timezone, p_timezone_offset,
                p_source_system, CURRENT_TIMESTAMP, TRUE
            ) RETURNING location_key INTO v_new_key;

            RETURN v_new_key;
        ELSE
            -- No change, return existing key
            RETURN v_existing_key;
        END IF;
    ELSE
        -- Insert new location
        INSERT INTO silver.locations (
            location_id, location_name, latitude, longitude,
            country_code, timezone, timezone_offset_seconds,
            source_system, effective_from, is_current
        ) VALUES (
            p_location_id, p_location_name, p_latitude, p_longitude,
            p_country_code, p_timezone, p_timezone_offset,
            p_source_system, CURRENT_TIMESTAMP, TRUE
        ) RETURNING location_key INTO v_new_key;

        RETURN v_new_key;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION silver.upsert_location IS 'Insert or update location with SCD Type 2 logic';

-- ============================================================================
-- SECTION 10: GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to roles
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO dw_developer, airflow_service;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO dw_analyst;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO dw_developer, airflow_service;
GRANT SELECT ON ALL VIEWS IN SCHEMA silver TO dw_developer, dw_analyst, dw_readonly;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA silver TO dw_developer, airflow_service;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Silver Layer DDL completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - silver.locations (SCD Type 2)';
    RAISE NOTICE '  - silver.weather_conditions (reference)';
    RAISE NOTICE '  - silver.weather_observations (partitioned)';
    RAISE NOTICE '  - silver.data_anomalies';
    RAISE NOTICE '';
    RAISE NOTICE 'Partitions created for: 2025-09, 2025-10, 2025-11, 2025-12';
    RAISE NOTICE 'Views created: vw_current_weather, vw_daily_weather_summary';
    RAISE NOTICE 'Functions created: Temperature conversions, Wind calculations, SCD upsert';
    RAISE NOTICE '';
    RAISE NOTICE 'Next step: Run ddl_gold.sql to create Gold layer dimensional model';
    RAISE NOTICE '============================================================================';
END $$;
