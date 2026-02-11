CREATE SCHEMA IF NOT EXISTS gold;

-- Daily weather summary aggregated from silver hourly observations
CREATE TABLE IF NOT EXISTS gold.daily_weather_summary (
    observation_date DATE NOT NULL,
    location_name VARCHAR(100) NOT NULL,

    -- Temperature
    avg_temperature_2m_c NUMERIC(5, 2),
    min_temperature_2m_c NUMERIC(5, 2),
    max_temperature_2m_c NUMERIC(5, 2),
    avg_apparent_temp_2m_c NUMERIC(5, 2),

    -- Precipitation
    total_precipitation_mm NUMERIC(6, 2),
    total_rain_mm NUMERIC(6, 2),
    total_showers_mm NUMERIC(6, 2),
    total_snowfall_mm NUMERIC(6, 2),

    -- Atmosphere
    avg_humidity_percent NUMERIC(5, 2),
    avg_wind_speed_kmh NUMERIC(5, 2),
    max_wind_speed_kmh NUMERIC(5, 2),
    avg_cloud_cover_percent NUMERIC(5, 2),
    max_uv_index NUMERIC(4, 2),

    -- Derived
    daylight_hours INT,
    dominant_weather_code VARCHAR(50),
    record_count INT,

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (observation_date, location_name)
);

CREATE INDEX IF NOT EXISTS idx_daily_weather_location_name
    ON gold.daily_weather_summary (location_name);

CREATE INDEX IF NOT EXISTS idx_daily_weather_date
    ON gold.daily_weather_summary (observation_date);

-- Aggregation: silver hourly -> gold daily
INSERT INTO gold.daily_weather_summary (
    observation_date,
    location_name,
    avg_temperature_2m_c,
    min_temperature_2m_c,
    max_temperature_2m_c,
    avg_apparent_temp_2m_c,
    total_precipitation_mm,
    total_rain_mm,
    total_showers_mm,
    total_snowfall_mm,
    avg_humidity_percent,
    avg_wind_speed_kmh,
    max_wind_speed_kmh,
    avg_cloud_cover_percent,
    max_uv_index,
    daylight_hours,
    dominant_weather_code,
    record_count
)
SELECT
    observation_timestamp::DATE AS observation_date,
    location_name,
    ROUND(AVG(temperature_2m_celsius)::NUMERIC, 2)       AS avg_temperature_2m_c,
    ROUND(MIN(temperature_2m_celsius)::NUMERIC, 2)       AS min_temperature_2m_c,
    ROUND(MAX(temperature_2m_celsius)::NUMERIC, 2)       AS max_temperature_2m_c,
    ROUND(AVG(apparent_temperature_celsius)::NUMERIC, 2) AS avg_apparent_temp_2m_c,
    ROUND(SUM(precipitation_mm)::NUMERIC, 2)             AS total_precipitation_mm,
    ROUND(SUM(rain_mm)::NUMERIC, 2)                      AS total_rain_mm,
    ROUND(SUM(showers_mm)::NUMERIC, 2)                   AS total_showers_mm,
    ROUND(SUM(snowfall_mm)::NUMERIC, 2)                  AS total_snowfall_mm,
    ROUND(AVG(relative_humidity_2m_percent)::NUMERIC, 2) AS avg_humidity_percent,
    ROUND(AVG(wind_speed_10m_kmh)::NUMERIC, 2)          AS avg_wind_speed_kmh,
    ROUND(MAX(wind_speed_10m_kmh)::NUMERIC, 2)          AS max_wind_speed_kmh,
    ROUND(AVG(cloud_cover_percent)::NUMERIC, 2)          AS avg_cloud_cover_percent,
    ROUND(MAX(uv_index)::NUMERIC, 2)                     AS max_uv_index,
    SUM(is_day)::INT                                     AS daylight_hours,
    MODE() WITHIN GROUP (ORDER BY weather_code)          AS dominant_weather_code,
    COUNT(*)                                             AS record_count
FROM silver.weather_observations
WHERE observation_timestamp IS NOT NULL
-- DATE_FILTER_PLACEHOLDER
GROUP BY observation_timestamp::DATE, location_name
ON CONFLICT (observation_date, location_name)
DO UPDATE SET
    avg_temperature_2m_c    = EXCLUDED.avg_temperature_2m_c,
    min_temperature_2m_c    = EXCLUDED.min_temperature_2m_c,
    max_temperature_2m_c    = EXCLUDED.max_temperature_2m_c,
    avg_apparent_temp_2m_c  = EXCLUDED.avg_apparent_temp_2m_c,
    total_precipitation_mm  = EXCLUDED.total_precipitation_mm,
    total_rain_mm           = EXCLUDED.total_rain_mm,
    total_showers_mm        = EXCLUDED.total_showers_mm,
    total_snowfall_mm       = EXCLUDED.total_snowfall_mm,
    avg_humidity_percent    = EXCLUDED.avg_humidity_percent,
    avg_wind_speed_kmh      = EXCLUDED.avg_wind_speed_kmh,
    max_wind_speed_kmh      = EXCLUDED.max_wind_speed_kmh,
    avg_cloud_cover_percent = EXCLUDED.avg_cloud_cover_percent,
    max_uv_index            = EXCLUDED.max_uv_index,
    daylight_hours          = EXCLUDED.daylight_hours,
    dominant_weather_code   = EXCLUDED.dominant_weather_code,
    record_count            = EXCLUDED.record_count,
    created_at              = NOW();
