CREATE TABLE IF NOT EXISTS silver.weather_observations (
    id SERIAL PRIMARY KEY,

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
    is_day BOOLEAN,

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
        UNIQUE (latitude, longitude, observation_timestamp)
);

-- Common query patterns index
CREATE INDEX idx_weather_obs_timestamp
    ON silver.weather_observations(observation_timestamp);

CREATE INDEX idx_weather_obs_location
    ON silver.weather_observations(latitude, longitude);


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

-- TODO: TRANSFORMATION
INSERT INTO silver.weather_observations (
    bronze_record_id,
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

);


-- EDA

SELECT raw_api_response -> 'hourly'
FROM bronze.weather_raw;

SELECT 
    record_id,
    pg_typeof(raw_api_response) as actual_type,
    raw_api_response
FROM bronze.weather_raw
LIMIT 1;

-- See how the column was defined
SELECT 
    column_name, 
    data_type, 
    udt_name
FROM information_schema.columns 
WHERE table_schema = 'bronze' 
  AND table_name = 'weather_raw';

-- List top-level keys
SELECT DISTINCT jsonb_object_keys(raw_api_response) as top_level_keys
FROM bronze.weather_raw;

-- List top-level keys from hourly variable
SELECT DISTINCT jsonb_object_keys(raw_api_response -> 'hourly') as hourly_level_keys
FROM bronze.weather_raw;


SELECT
    record_id as bronze_record_id,
    (raw_api_response ->> 'latitude')::FLOAT as latitude,
    (raw_api_response ->> 'longitude')::FLOAT as longitude,
    time_value::TIMESTAMPTZ as observation_timestamp,
    temp_value::FLOAT as temperature_2m_celsius,
    apparent_temp_value::FLOAT as apparent_temperature_celsius,
    humidity_value::FLOAT as relative_humidity_2m_percent,
    precipitation_value::FLOAT as precipitation_mm,
    weather_code_value as weather_code,
    is_day_value as is_day,
    wind_speed_value::FLOAT as wind_speed_10m_kmh
FROM bronze.weather_raw,
     UNNEST(
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'time')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'temperature_2m')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'apparent_temperature')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'relative_humidity_2m')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'precipitation')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'weather_code')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'is_day')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'wind_speed_10m'))
     ) AS t(time_value, temp_value, apparent_temp_value, humidity_value, precipitation_value, weather_code_value, is_day_value, wind_speed_value)
WHERE is_day_value IS NOT NULL
LIMIT 100;