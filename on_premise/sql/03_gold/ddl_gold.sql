-- ============================================================================
-- Gold Layer DDL - Business-Ready Dimensional Model (Star Schema)
-- ============================================================================
-- Purpose: Optimized for business intelligence and analytics
-- Schema: gold
-- Data Format: Dimensional model (fact and dimension tables)
-- Architecture: Star Schema with slowly changing dimensions
-- ============================================================================

-- Connect to warehouse database
\c warehouse_db

-- Set search path
SET search_path TO gold, silver, public;

-- ============================================================================
-- SECTION 1: DROP EXISTING TABLES (Use with caution!)
-- ============================================================================
-- Uncomment to recreate tables from scratch
-- DROP TABLE IF EXISTS gold.fact_weather_hourly CASCADE;
-- DROP TABLE IF EXISTS gold.fact_weather_daily CASCADE;
-- DROP TABLE IF EXISTS gold.dim_date CASCADE;
-- DROP TABLE IF EXISTS gold.dim_time CASCADE;
-- DROP TABLE IF EXISTS gold.dim_location CASCADE;
-- DROP TABLE IF EXISTS gold.dim_weather_condition CASCADE;

-- ============================================================================
-- SECTION 2: CREATE DATE DIMENSION
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,

    -- Date parts
    day_of_month INTEGER NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_of_week_name VARCHAR(20) NOT NULL,
    day_of_week_abbr VARCHAR(3) NOT NULL,
    day_of_year INTEGER NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),

    -- Week
    week_of_year INTEGER NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    iso_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,

    -- Month
    month_number INTEGER NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name VARCHAR(20) NOT NULL,
    month_abbr VARCHAR(3) NOT NULL,
    first_day_of_month DATE NOT NULL,
    last_day_of_month DATE NOT NULL,

    -- Quarter
    quarter_number INTEGER NOT NULL CHECK (quarter_number BETWEEN 1 AND 4),
    quarter_name VARCHAR(10) NOT NULL,
    first_day_of_quarter DATE NOT NULL,
    last_day_of_quarter DATE NOT NULL,

    -- Year
    year INTEGER NOT NULL,
    is_leap_year BOOLEAN NOT NULL,
    year_quarter VARCHAR(10) NOT NULL,  -- 2025-Q1
    year_month VARCHAR(10) NOT NULL,    -- 2025-01

    -- Season (Northern Hemisphere)
    season VARCHAR(20) NOT NULL,

    -- Business day flags
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(255),
    is_business_day BOOLEAN NOT NULL,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for date dimension
CREATE INDEX IF NOT EXISTS idx_gold_dim_date_full
    ON gold.dim_date(full_date);

CREATE INDEX IF NOT EXISTS idx_gold_dim_date_year_month
    ON gold.dim_date(year, month_number);

CREATE INDEX IF NOT EXISTS idx_gold_dim_date_quarter
    ON gold.dim_date(year, quarter_number);

COMMENT ON TABLE gold.dim_date IS 'Date dimension for time-based analysis';

-- ============================================================================
-- SECTION 3: CREATE TIME DIMENSION
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_time (
    time_key INTEGER PRIMARY KEY,  -- Format: HHMMSS (e.g., 143045 for 14:30:45)
    full_time TIME NOT NULL UNIQUE,

    -- Time parts
    hour INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    minute INTEGER NOT NULL CHECK (minute BETWEEN 0 AND 59),
    second INTEGER NOT NULL CHECK (second BETWEEN 0 AND 59),

    -- Hour classification
    hour_12 INTEGER NOT NULL CHECK (hour_12 BETWEEN 1 AND 12),
    am_pm VARCHAR(2) NOT NULL,
    hour_name VARCHAR(20) NOT NULL,  -- '2 PM', '14:00'

    -- Time of day classification
    time_of_day VARCHAR(20) NOT NULL,  -- 'Morning', 'Afternoon', 'Evening', 'Night'
    is_business_hours BOOLEAN NOT NULL,

    -- Time blocks (for analysis)
    hour_block VARCHAR(20) NOT NULL,  -- '00-03', '04-07', etc.
    minute_block INTEGER NOT NULL,    -- 0, 15, 30, 45

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for time dimension
CREATE INDEX IF NOT EXISTS idx_gold_dim_time_hour
    ON gold.dim_time(hour);

CREATE INDEX IF NOT EXISTS idx_gold_dim_time_of_day
    ON gold.dim_time(time_of_day);

COMMENT ON TABLE gold.dim_time IS 'Time dimension for hourly analysis';

-- ============================================================================
-- SECTION 4: CREATE LOCATION DIMENSION
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key INTEGER PRIMARY KEY,
    location_id VARCHAR(100) NOT NULL,  -- Natural key

    -- Location attributes
    location_name VARCHAR(255) NOT NULL,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(10, 7) NOT NULL,

    -- Geographic hierarchy
    city VARCHAR(255),
    state_province VARCHAR(255),
    country VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    region VARCHAR(100),
    continent VARCHAR(50),

    -- Time zone
    timezone VARCHAR(100),
    timezone_offset_hours DECIMAL(4, 2),

    -- Geographic classification
    climate_zone VARCHAR(100),
    elevation_meters INTEGER,
    population_category VARCHAR(50),  -- 'Small', 'Medium', 'Large', 'Mega'
    location_type VARCHAR(50),        -- 'Urban', 'Suburban', 'Rural'

    -- Coordinates for mapping
    geom_point POINT,

    -- SCD Type 2 columns
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    data_quality_score DECIMAL(5, 2),
    source_system VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Indexes for location dimension
CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_dim_location_current
    ON gold.dim_location(location_id, is_current)
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_gold_dim_location_country
    ON gold.dim_location(country_code, country);

CREATE INDEX IF NOT EXISTS idx_gold_dim_location_coords
    ON gold.dim_location(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_gold_dim_location_region
    ON gold.dim_location(region, continent);

COMMENT ON TABLE gold.dim_location IS 'Location dimension with geographic hierarchy and SCD Type 2';

-- ============================================================================
-- SECTION 5: CREATE WEATHER CONDITION DIMENSION
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_weather_condition (
    condition_key INTEGER PRIMARY KEY,
    condition_id INTEGER UNIQUE NOT NULL,

    -- Condition details
    condition_name VARCHAR(100) NOT NULL,
    condition_description VARCHAR(255) NOT NULL,
    condition_icon VARCHAR(20),

    -- Hierarchical classification
    condition_category VARCHAR(100) NOT NULL,     -- Level 1: 'Clear', 'Clouds', 'Rain', 'Snow'
    condition_subcategory VARCHAR(100),           -- Level 2: 'Light Rain', 'Heavy Rain'
    condition_group VARCHAR(100) NOT NULL,        -- Level 3: 'Precipitation', 'Clear Sky'

    -- Severity and impact
    severity_level VARCHAR(50) NOT NULL,          -- 'Low', 'Medium', 'High', 'Extreme'
    severity_score INTEGER CHECK (severity_score BETWEEN 0 AND 10),
    impact_on_activities VARCHAR(255),            -- Brief description of impact

    -- Weather alerts
    requires_alert BOOLEAN DEFAULT FALSE,
    alert_type VARCHAR(100),

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Indexes for weather condition dimension
CREATE INDEX IF NOT EXISTS idx_gold_dim_condition_category
    ON gold.dim_weather_condition(condition_category);

CREATE INDEX IF NOT EXISTS idx_gold_dim_condition_severity
    ON gold.dim_weather_condition(severity_level);

COMMENT ON TABLE gold.dim_weather_condition IS 'Weather condition dimension with hierarchical classification';

-- ============================================================================
-- SECTION 6: CREATE HOURLY WEATHER FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly (
    fact_id BIGSERIAL,
    observation_timestamp TIMESTAMP NOT NULL,

    -- Dimension foreign keys
    date_key INTEGER NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    condition_key INTEGER,

    -- Temperature measures
    temperature_celsius DECIMAL(6, 2) NOT NULL,
    temperature_fahrenheit DECIMAL(6, 2) NOT NULL,
    feels_like_celsius DECIMAL(6, 2),
    feels_like_fahrenheit DECIMAL(6, 2),
    temperature_min_celsius DECIMAL(6, 2),
    temperature_max_celsius DECIMAL(6, 2),
    heat_index DECIMAL(6, 2),
    wind_chill DECIMAL(6, 2),
    dew_point DECIMAL(6, 2),

    -- Atmospheric measures
    pressure_hpa INTEGER,
    humidity_percent INTEGER NOT NULL,
    visibility_km DECIMAL(6, 2),

    -- Wind measures
    wind_speed_kmh DECIMAL(6, 2),
    wind_direction_degrees INTEGER,
    wind_gust_kmh DECIMAL(6, 2),
    beaufort_scale INTEGER,

    -- Precipitation measures (hourly)
    rain_mm DECIMAL(6, 2),
    snow_mm DECIMAL(6, 2),
    precipitation_total_mm DECIMAL(6, 2),
    precipitation_type VARCHAR(50),  -- 'None', 'Rain', 'Snow', 'Mixed'

    -- Cloud and sun measures
    cloudiness_percent INTEGER,
    daylight_hours DECIMAL(4, 2),
    uv_index DECIMAL(4, 2),

    -- Air quality
    air_quality_index INTEGER,
    air_quality_category VARCHAR(50),  -- 'Good', 'Moderate', 'Unhealthy', etc.

    -- Derived metrics
    comfort_index DECIMAL(5, 2),       -- Custom comfort metric (0-100)
    weather_severity_score INTEGER,    -- Composite severity score (0-10)

    -- Data quality
    observation_count INTEGER DEFAULT 1,
    quality_score DECIMAL(5, 2),
    has_anomaly BOOLEAN DEFAULT FALSE,

    -- Metadata
    source_system VARCHAR(100) NOT NULL,
    processed_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_version INTEGER DEFAULT 1,

    -- Partition key
    observation_date DATE NOT NULL,

    -- Primary key
    PRIMARY KEY (fact_id, observation_date),

    -- Foreign keys
    CONSTRAINT fk_fact_hourly_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_hourly_time FOREIGN KEY (time_key)
        REFERENCES gold.dim_time(time_key),
    CONSTRAINT fk_fact_hourly_location FOREIGN KEY (location_key)
        REFERENCES gold.dim_location(location_key),
    CONSTRAINT fk_fact_hourly_condition FOREIGN KEY (condition_key)
        REFERENCES gold.dim_weather_condition(condition_key)
) PARTITION BY RANGE (observation_date);

-- ============================================================================
-- SECTION 7: CREATE PARTITIONS FOR HOURLY FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly_2025_09
    PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly_2025_10
    PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly_2025_11
    PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly_2025_12
    PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly_default
    PARTITION OF gold.fact_weather_hourly
    DEFAULT;

-- ============================================================================
-- SECTION 8: CREATE INDEXES FOR HOURLY FACT TABLE
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_gold_fact_hourly_date_location
    ON gold.fact_weather_hourly(date_key, location_key);

CREATE INDEX IF NOT EXISTS idx_gold_fact_hourly_timestamp
    ON gold.fact_weather_hourly(observation_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gold_fact_hourly_location_time
    ON gold.fact_weather_hourly(location_key, time_key);

CREATE INDEX IF NOT EXISTS idx_gold_fact_hourly_condition
    ON gold.fact_weather_hourly(condition_key);

CREATE INDEX IF NOT EXISTS idx_gold_fact_hourly_temp
    ON gold.fact_weather_hourly(temperature_celsius);

COMMENT ON TABLE gold.fact_weather_hourly IS 'Hourly weather observations fact table optimized for BI queries';

-- ============================================================================
-- SECTION 9: CREATE DAILY WEATHER FACT TABLE (Aggregate)
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_weather_daily (
    daily_fact_id BIGSERIAL PRIMARY KEY,
    observation_date DATE NOT NULL,

    -- Dimension foreign keys
    date_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    dominant_condition_key INTEGER,  -- Most frequent condition of the day

    -- Temperature aggregates
    avg_temperature_celsius DECIMAL(6, 2) NOT NULL,
    avg_temperature_fahrenheit DECIMAL(6, 2) NOT NULL,
    min_temperature_celsius DECIMAL(6, 2) NOT NULL,
    max_temperature_celsius DECIMAL(6, 2) NOT NULL,
    temperature_range_celsius DECIMAL(6, 2),
    avg_feels_like_celsius DECIMAL(6, 2),
    avg_heat_index DECIMAL(6, 2),
    avg_wind_chill DECIMAL(6, 2),
    avg_dew_point DECIMAL(6, 2),

    -- Atmospheric aggregates
    avg_pressure_hpa INTEGER,
    min_pressure_hpa INTEGER,
    max_pressure_hpa INTEGER,
    avg_humidity_percent INTEGER NOT NULL,
    min_humidity_percent INTEGER,
    max_humidity_percent INTEGER,
    avg_visibility_km DECIMAL(6, 2),

    -- Wind aggregates
    avg_wind_speed_kmh DECIMAL(6, 2),
    max_wind_speed_kmh DECIMAL(6, 2),
    min_wind_speed_kmh DECIMAL(6, 2),
    max_wind_gust_kmh DECIMAL(6, 2),
    dominant_wind_direction VARCHAR(5),

    -- Precipitation aggregates
    total_rain_mm DECIMAL(6, 2),
    total_snow_mm DECIMAL(6, 2),
    total_precipitation_mm DECIMAL(6, 2),
    precipitation_hours INTEGER,  -- Number of hours with precipitation
    max_hourly_rain_mm DECIMAL(6, 2),

    -- Cloud and sun aggregates
    avg_cloudiness_percent INTEGER,
    total_daylight_hours DECIMAL(4, 2),
    avg_uv_index DECIMAL(4, 2),
    max_uv_index DECIMAL(4, 2),
    sunrise_time TIME,
    sunset_time TIME,

    -- Air quality
    avg_air_quality_index INTEGER,
    max_air_quality_index INTEGER,
    dominant_air_quality_category VARCHAR(50),

    -- Derived daily metrics
    avg_comfort_index DECIMAL(5, 2),
    max_weather_severity_score INTEGER,
    weather_summary VARCHAR(255),  -- Brief text summary

    -- Extreme weather flags
    had_extreme_heat BOOLEAN DEFAULT FALSE,
    had_extreme_cold BOOLEAN DEFAULT FALSE,
    had_heavy_precipitation BOOLEAN DEFAULT FALSE,
    had_strong_wind BOOLEAN DEFAULT FALSE,

    -- Data quality
    observation_count INTEGER,
    completeness_percent DECIMAL(5, 2),  -- % of expected hourly observations
    avg_quality_score DECIMAL(5, 2),
    has_anomaly BOOLEAN DEFAULT FALSE,

    -- Metadata
    source_system VARCHAR(100) NOT NULL,
    processed_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_version INTEGER DEFAULT 1,

    -- Foreign keys
    CONSTRAINT fk_fact_daily_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_daily_location FOREIGN KEY (location_key)
        REFERENCES gold.dim_location(location_key),
    CONSTRAINT fk_fact_daily_condition FOREIGN KEY (dominant_condition_key)
        REFERENCES gold.dim_weather_condition(condition_key),

    -- Unique constraint
    CONSTRAINT uq_fact_daily_date_location UNIQUE (observation_date, location_key)
);

-- Indexes for daily fact table
CREATE INDEX IF NOT EXISTS idx_gold_fact_daily_date_location
    ON gold.fact_weather_daily(date_key, location_key);

CREATE INDEX IF NOT EXISTS idx_gold_fact_daily_date
    ON gold.fact_weather_daily(observation_date DESC);

CREATE INDEX IF NOT EXISTS idx_gold_fact_daily_location
    ON gold.fact_weather_daily(location_key, observation_date DESC);

CREATE INDEX IF NOT EXISTS idx_gold_fact_daily_temp
    ON gold.fact_weather_daily(avg_temperature_celsius);

CREATE INDEX IF NOT EXISTS idx_gold_fact_daily_extremes
    ON gold.fact_weather_daily(had_extreme_heat, had_extreme_cold, had_heavy_precipitation);

COMMENT ON TABLE gold.fact_weather_daily IS 'Daily aggregated weather fact table for trend analysis';

-- ============================================================================
-- SECTION 10: CREATE MONTHLY WEATHER FACT TABLE (Aggregate)
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_weather_monthly (
    monthly_fact_id BIGSERIAL PRIMARY KEY,
    observation_year INTEGER NOT NULL,
    observation_month INTEGER NOT NULL,
    year_month VARCHAR(10) NOT NULL,  -- YYYY-MM

    -- Dimension foreign keys
    location_key INTEGER NOT NULL,

    -- Temperature statistics
    avg_temperature_celsius DECIMAL(6, 2) NOT NULL,
    min_temperature_celsius DECIMAL(6, 2) NOT NULL,
    max_temperature_celsius DECIMAL(6, 2) NOT NULL,
    temperature_range_celsius DECIMAL(6, 2),
    std_dev_temperature DECIMAL(6, 2),

    -- Precipitation statistics
    total_precipitation_mm DECIMAL(6, 2),
    total_rain_mm DECIMAL(6, 2),
    total_snow_mm DECIMAL(6, 2),
    rainy_days_count INTEGER,
    snowy_days_count INTEGER,
    max_daily_precipitation_mm DECIMAL(6, 2),

    -- Wind statistics
    avg_wind_speed_kmh DECIMAL(6, 2),
    max_wind_speed_kmh DECIMAL(6, 2),
    max_wind_gust_kmh DECIMAL(6, 2),

    -- Other statistics
    avg_humidity_percent INTEGER,
    avg_pressure_hpa INTEGER,
    avg_cloudiness_percent INTEGER,
    total_daylight_hours DECIMAL(6, 2),

    -- Extreme weather counts
    extreme_heat_days INTEGER DEFAULT 0,
    extreme_cold_days INTEGER DEFAULT 0,
    heavy_precipitation_days INTEGER DEFAULT 0,
    strong_wind_days INTEGER DEFAULT 0,

    -- Data quality
    days_with_data INTEGER,
    data_completeness_percent DECIMAL(5, 2),

    -- Metadata
    processed_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign keys
    CONSTRAINT fk_fact_monthly_location FOREIGN KEY (location_key)
        REFERENCES gold.dim_location(location_key),

    -- Unique constraint
    CONSTRAINT uq_fact_monthly_year_month_location
        UNIQUE (observation_year, observation_month, location_key)
);

-- Indexes for monthly fact table
CREATE INDEX IF NOT EXISTS idx_gold_fact_monthly_year_month
    ON gold.fact_weather_monthly(observation_year, observation_month);

CREATE INDEX IF NOT EXISTS idx_gold_fact_monthly_location
    ON gold.fact_weather_monthly(location_key, observation_year, observation_month);

COMMENT ON TABLE gold.fact_weather_monthly IS 'Monthly aggregated weather statistics';

-- ============================================================================
-- SECTION 11: CREATE BUSINESS VIEWS
-- ============================================================================

-- View: Current weather dashboard
CREATE OR REPLACE VIEW gold.vw_current_weather_dashboard AS
SELECT
    l.location_name,
    l.city,
    l.country,
    l.latitude,
    l.longitude,
    f.observation_timestamp,
    f.temperature_celsius,
    f.temperature_fahrenheit,
    f.feels_like_celsius,
    f.humidity_percent,
    f.pressure_hpa,
    f.wind_speed_kmh,
    wc.condition_description,
    wc.condition_icon,
    wc.severity_level,
    f.precipitation_total_mm,
    f.cloudiness_percent,
    f.uv_index,
    f.quality_score
FROM gold.fact_weather_hourly f
JOIN gold.dim_location l ON f.location_key = l.location_key
LEFT JOIN gold.dim_weather_condition wc ON f.condition_key = wc.condition_key
WHERE l.is_current = TRUE
    AND f.observation_timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
    AND f.quality_score >= 70
ORDER BY f.observation_timestamp DESC;

COMMENT ON VIEW gold.vw_current_weather_dashboard IS 'Real-time weather dashboard with latest observations';

-- View: Daily weather summary
CREATE OR REPLACE VIEW gold.vw_daily_weather_summary AS
SELECT
    d.full_date,
    d.day_of_week_name,
    l.location_name,
    l.country,
    f.avg_temperature_celsius,
    f.min_temperature_celsius,
    f.max_temperature_celsius,
    f.total_precipitation_mm,
    f.avg_humidity_percent,
    f.avg_wind_speed_kmh,
    f.max_wind_speed_kmh,
    wc.condition_description as dominant_condition,
    f.weather_summary,
    f.had_extreme_heat,
    f.had_extreme_cold,
    f.had_heavy_precipitation,
    f.completeness_percent
FROM gold.fact_weather_daily f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_location l ON f.location_key = l.location_key
LEFT JOIN gold.dim_weather_condition wc ON f.dominant_condition_key = wc.condition_key
WHERE l.is_current = TRUE
ORDER BY d.full_date DESC, l.location_name;

COMMENT ON VIEW gold.vw_daily_weather_summary IS 'Daily weather summary for business reporting';

-- View: Temperature trends by location
CREATE OR REPLACE VIEW gold.vw_temperature_trends AS
SELECT
    l.location_name,
    l.country,
    d.year,
    d.month_name,
    AVG(f.avg_temperature_celsius) as avg_monthly_temp_celsius,
    MIN(f.min_temperature_celsius) as min_monthly_temp_celsius,
    MAX(f.max_temperature_celsius) as max_monthly_temp_celsius,
    AVG(f.avg_humidity_percent) as avg_monthly_humidity,
    SUM(f.total_precipitation_mm) as total_monthly_precipitation_mm
FROM gold.fact_weather_daily f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_location l ON f.location_key = l.location_key
WHERE l.is_current = TRUE
GROUP BY l.location_name, l.country, d.year, d.month_number, d.month_name
ORDER BY d.year DESC, d.month_number DESC, l.location_name;

COMMENT ON VIEW gold.vw_temperature_trends IS 'Monthly temperature trends for climate analysis';

-- ============================================================================
-- SECTION 12: CREATE UTILITY FUNCTIONS
-- ============================================================================

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION gold.populate_date_dimension(
    p_start_date DATE,
    p_end_date DATE
)
RETURNS INTEGER AS $$
DECLARE
    v_current_date DATE;
    v_count INTEGER := 0;
BEGIN
    v_current_date := p_start_date;

    WHILE v_current_date <= p_end_date LOOP
        INSERT INTO gold.dim_date (
            date_key, full_date, day_of_month, day_of_week,
            day_of_week_name, day_of_week_abbr, day_of_year,
            week_of_year, iso_week, is_weekend,
            month_number, month_name, month_abbr,
            first_day_of_month, last_day_of_month,
            quarter_number, quarter_name,
            first_day_of_quarter, last_day_of_quarter,
            year, is_leap_year, year_quarter, year_month, season,
            is_business_day
        ) VALUES (
            TO_CHAR(v_current_date, 'YYYYMMDD')::INTEGER,
            v_current_date,
            EXTRACT(DAY FROM v_current_date),
            EXTRACT(ISODOW FROM v_current_date),
            TO_CHAR(v_current_date, 'Day'),
            TO_CHAR(v_current_date, 'Dy'),
            EXTRACT(DOY FROM v_current_date),
            EXTRACT(WEEK FROM v_current_date),
            EXTRACT(WEEK FROM v_current_date),
            EXTRACT(ISODOW FROM v_current_date) IN (6, 7),
            EXTRACT(MONTH FROM v_current_date),
            TO_CHAR(v_current_date, 'Month'),
            TO_CHAR(v_current_date, 'Mon'),
            DATE_TRUNC('month', v_current_date)::DATE,
            (DATE_TRUNC('month', v_current_date) + INTERVAL '1 month - 1 day')::DATE,
            EXTRACT(QUARTER FROM v_current_date),
            'Q' || EXTRACT(QUARTER FROM v_current_date),
            DATE_TRUNC('quarter', v_current_date)::DATE,
            (DATE_TRUNC('quarter', v_current_date) + INTERVAL '3 months - 1 day')::DATE,
            EXTRACT(YEAR FROM v_current_date),
            (EXTRACT(YEAR FROM v_current_date)::INTEGER % 4 = 0 AND
             (EXTRACT(YEAR FROM v_current_date)::INTEGER % 100 != 0 OR
              EXTRACT(YEAR FROM v_current_date)::INTEGER % 400 = 0)),
            EXTRACT(YEAR FROM v_current_date)::TEXT || '-Q' || EXTRACT(QUARTER FROM v_current_date)::TEXT,
            TO_CHAR(v_current_date, 'YYYY-MM'),
            CASE EXTRACT(MONTH FROM v_current_date)
                WHEN 12, 1, 2 THEN 'Winter'
                WHEN 3, 4, 5 THEN 'Spring'
                WHEN 6, 7, 8 THEN 'Summer'
                WHEN 9, 10, 11 THEN 'Fall'
            END,
            EXTRACT(ISODOW FROM v_current_date) NOT IN (6, 7)
        )
        ON CONFLICT (date_key) DO NOTHING;

        v_count := v_count + 1;
        v_current_date := v_current_date + INTERVAL '1 day';
    END LOOP;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gold.populate_date_dimension IS 'Populates date dimension for specified date range';

-- Populate date dimension for 5 years (2023-2027)
SELECT gold.populate_date_dimension('2023-01-01'::DATE, '2027-12-31'::DATE);

-- Function to populate time dimension
CREATE OR REPLACE FUNCTION gold.populate_time_dimension()
RETURNS INTEGER AS $$
DECLARE
    v_hour INTEGER;
    v_count INTEGER := 0;
BEGIN
    FOR v_hour IN 0..23 LOOP
        INSERT INTO gold.dim_time (
            time_key, full_time, hour, minute, second,
            hour_12, am_pm, hour_name, time_of_day,
            is_business_hours, hour_block, minute_block
        ) VALUES (
            v_hour * 10000,
            (v_hour || ':00:00')::TIME,
            v_hour,
            0,
            0,
            CASE WHEN v_hour = 0 THEN 12 WHEN v_hour > 12 THEN v_hour - 12 ELSE v_hour END,
            CASE WHEN v_hour < 12 THEN 'AM' ELSE 'PM' END,
            TO_CHAR((v_hour || ':00:00')::TIME, 'HH12 AM'),
            CASE
                WHEN v_hour >= 6 AND v_hour < 12 THEN 'Morning'
                WHEN v_hour >= 12 AND v_hour < 17 THEN 'Afternoon'
                WHEN v_hour >= 17 AND v_hour < 21 THEN 'Evening'
                ELSE 'Night'
            END,
            v_hour >= 9 AND v_hour < 17,
            LPAD((v_hour / 4 * 4)::TEXT, 2, '0') || '-' || LPAD(((v_hour / 4 * 4) + 3)::TEXT, 2, '0'),
            0
        )
        ON CONFLICT (time_key) DO NOTHING;

        v_count := v_count + 1;
    END LOOP;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gold.populate_time_dimension IS 'Populates time dimension with hourly records';

-- Populate time dimension
SELECT gold.populate_time_dimension();

-- ============================================================================
-- SECTION 13: GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to roles
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_developer, dw_analyst, dw_readonly;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO airflow_service;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO airflow_service;
GRANT SELECT ON ALL VIEWS IN SCHEMA gold TO dw_developer, dw_analyst, dw_readonly;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gold TO dw_developer, airflow_service;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Gold Layer DDL completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Dimension Tables:';
    RAISE NOTICE '  - gold.dim_date (populated)';
    RAISE NOTICE '  - gold.dim_time (populated)';
    RAISE NOTICE '  - gold.dim_location';
    RAISE NOTICE '  - gold.dim_weather_condition';
    RAISE NOTICE '';
    RAISE NOTICE 'Fact Tables:';
    RAISE NOTICE '  - gold.fact_weather_hourly (partitioned)';
    RAISE NOTICE '  - gold.fact_weather_daily';
    RAISE NOTICE '  - gold.fact_weather_monthly';
    RAISE NOTICE '';
    RAISE NOTICE 'Business Views:';
    RAISE NOTICE '  - vw_current_weather_dashboard';
    RAISE NOTICE '  - vw_daily_weather_summary';
    RAISE NOTICE '  - vw_temperature_trends';
    RAISE NOTICE '';
    RAISE NOTICE 'Star Schema complete! Ready for BI tools and analytics.';
    RAISE NOTICE '============================================================================';
END $$;
