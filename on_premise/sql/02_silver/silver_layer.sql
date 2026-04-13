CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.weather_observations (
    id SERIAL PRIMARY KEY,
    -- Multi City features
    location_name VARCHAR(100),

    -- Lineage
    bronze_record_id INTEGER,

    -- Metadata
    latitude FLOAT,
    longitude FLOAT,
    timezone VARCHAR(50),

    -- Timestamp
    observation_timestamp TIMESTAMPTZ NOT NULL,

    -- Core Metrics
    temperature_2m_celsius FLOAT,
    apparent_temperature_celsius FLOAT,
    relative_humidity_2m_percent FLOAT,
    precipitation_mm FLOAT,
    weather_code VARCHAR(50),
    is_day FLOAT,

    -- Metrics for analysis
    wind_speed_10m_kmh FLOAT,
    cloud_cover_percent FLOAT,
    uv_index FLOAT,

    -- Optional Features
    rain_mm FLOAT,
    showers_mm FLOAT,
    snowfall_mm FLOAT,

    -- Audit
    api_retrieval_time TIMESTAMPTZ,
    transformed_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_observation 
        UNIQUE (location_name, observation_timestamp)
);

--Common query patterns index (Uncomment if run for the first time)
CREATE INDEX IF NOT EXISTS idx_weather_obs_timestamp
    ON silver.weather_observations(observation_timestamp);

CREATE INDEX IF NOT EXISTS idx_weather_obs_location_time
    ON silver.weather_observations(location_name, observation_timestamp);

CREATE INDEX IF NOT EXISTS idx_weather_obs_location
    ON silver.weather_observations(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_weather_obs_location_name
    ON silver.weather_observations(location_name);


/*
            "temperature_2m", 
            "apparent_temperature", 
            "relative_humidity_2m", 
            "precipitation",
            "precipitation_probability", 
            "weather_code", 
            "wind_speed_10m", 
            "wind_direction_10m", 
            "surface_pressure", 
            "cloud_cover", 
            "rain", 
            "showers", 
            "snowfall", 
            "uv_index", 
            "is_day"
*/

-- (CTE)
WITH unnested AS (
    SELECT
        w.record_id as bronze_record_id,
        w.location_name,
        (w.raw_api_response->>'latitude')::FLOAT as latitude,
        (w.raw_api_response->>'longitude')::FLOAT as longitude,
        w.raw_api_response->>'timezone' as timezone,
        time_val::TIMESTAMPTZ as observation_timestamp,
        temp_val::FLOAT as temperature_2m_celsius,
        apparent_temp_val::FLOAT as apparent_temperature_celsius,
        humidity_val::FLOAT as relative_humidity_2m_percent,
        precip_val::FLOAT as precipitation_mm,
        weather_code_val as weather_code,
        is_day_val::FLOAT as is_day,
        wind_speed_val::FLOAT as wind_speed_10m_kmh,
        cloud_cover_val::FLOAT as cloud_cover_percent,
        uv_val::FLOAT as uv_index,
        rain_val::FLOAT as rain_mm,
        showers_val::FLOAT as showers_mm,
        snowfall_val::FLOAT as snowfall_mm,
        w.api_retrieval_time as api_retrieval_time
    FROM bronze.weather_raw w,
    UNNEST(
        -- Extract each array from the JSONB
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'time')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'temperature_2m')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'apparent_temperature')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'relative_humidity_2m')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'precipitation')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'weather_code')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'is_day')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'wind_speed_10m')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'cloud_cover')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'rain')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'showers')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'snowfall')),
        ARRAY(SELECT jsonb_array_elements_text(w.raw_api_response->'hourly'->'uv_index'))
    ) AS h(
        time_val, temp_val, apparent_temp_val, humidity_val, 
        precip_val, weather_code_val, is_day_val, wind_speed_val, 
        cloud_cover_val, rain_val, showers_val, snowfall_val, uv_val
        )
),
deduplicated AS (
    SELECT DISTINCT ON (location_name, observation_timestamp) *
    FROM unnested
    -- DATE_FILTER_PLACEHOLDER
    ORDER BY location_name, observation_timestamp, api_retrieval_time DESC
)

-- TRANSFORMATION
INSERT INTO silver.weather_observations (
    bronze_record_id,
    location_name,
    latitude,
    longitude,
    timezone,
    observation_timestamp,
    temperature_2m_celsius,
    apparent_temperature_celsius,
    relative_humidity_2m_percent,
    precipitation_mm,
    weather_code,
    is_day,
    wind_speed_10m_kmh,
    cloud_cover_percent,
    uv_index,
    rain_mm,
    showers_mm,
    snowfall_mm,
    api_retrieval_time
) 
SELECT
    bronze_record_id,
    location_name,
    latitude,
    longitude,
    timezone,
    observation_timestamp,
    temperature_2m_celsius,
    apparent_temperature_celsius,
    relative_humidity_2m_percent,
    precipitation_mm,
    weather_code,
    is_day,
    wind_speed_10m_kmh,
    cloud_cover_percent,
    uv_index,
    rain_mm,
    showers_mm,
    snowfall_mm,
    api_retrieval_time
FROM deduplicated
-- FIXED Slow Changing Dimension ISSUE
ON CONFLICT (location_name, observation_timestamp)
DO UPDATE SET
    temperature_2m_celsius = EXCLUDED.temperature_2m_celsius,
    apparent_temperature_celsius = EXCLUDED.apparent_temperature_celsius,
    relative_humidity_2m_percent = EXCLUDED.relative_humidity_2m_percent,
    precipitation_mm = EXCLUDED.precipitation_mm,
    weather_code = EXCLUDED.weather_code,
    is_day = EXCLUDED.is_day,
    wind_speed_10m_kmh = EXCLUDED.wind_speed_10m_kmh,
    cloud_cover_percent = EXCLUDED.cloud_cover_percent,
    uv_index = EXCLUDED.uv_index,
    rain_mm = EXCLUDED.rain_mm,
    showers_mm = EXCLUDED.showers_mm,
    snowfall_mm = EXCLUDED.snowfall_mm,
    bronze_record_id = EXCLUDED.bronze_record_id,
    api_retrieval_time = EXCLUDED.api_retrieval_time,
    transformed_at = NOW()
WHERE EXCLUDED.api_retrieval_time > silver.weather_observations.api_retrieval_time;