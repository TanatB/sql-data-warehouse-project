CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_weather_summary
(
    date DATE,
    location_id INTEGER,
    avg_temperature_2m_c NUMERIC(3, 1),
    min_temperature_2m_c NUMERIC(3, 1),
    max_temperature_2m_c NUMERIC(3, 1),
    total_precipitation_mm NUMERIC(4, 1),
    avg_apparent_temp_2m_c NUMERIC(3, 1),
    avg_humidity_percent NUMERIC(3, 1),
    total_rain_mm NUMERIC(4, 1),
    max_precipitation_probability INT,
    avg_wind_speed_kmh NUMERIC(3, 1),
    max_wind_speed_kmh NUMERIC(3, 1),
    avg_surface_pressure_hpa NUMERIC(4, 1),
    avg_cloud_cover_percent NUMERIC(3, 1),
    max_uv_index NUMERIC(3, 1),
    daylight_hours INT,
    dominant_weather_code FLOAT,
    record_count INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, location_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_weather_location 
ON gold.daily_weather_summary (location_id);

INSERT INTO gold.daily_weather_summary
SELECT
    date,
    location_id,
    AVG(temperature_2m_celsius) as avg_temperature_2m_c,
    MIN(temperature_2m_celsius) as min_temperature_2m_c,
    MAX(temperature_2m_celsius) as max_temperature_2m_c,
    SUM(precipitation_mm) as total_precipitation_mm,
    AVG(apparent_temperature_celsius) as avg_apparent_temp_2m_c,
    AVG(relative_humidity_2m_percent) as avg_humidity_percent,
    SUM(rain_mm) as total_rain_mm,
    MAX(precipitation_mm) as max_precipitation_probability,
    AVG(wind_speed_10m_kmh) as avg_wind_speed_kmh,
    MAX(wind_speed_10m_kmh) as max_wind_speed_kmh,
    AVG(cloud_cover_percent) as avg_cloud_cover_percent, 
    MAX(uv_index) as max_uv_index,
    SUM(is_day) as daylight_hours,
    MODE(weather_code) as dominant_weather_code,
    COUNT(*) as record_count
FROM silver.weather_observations
WHERE api_retrieval_time
GROUP BY date, location_id;
