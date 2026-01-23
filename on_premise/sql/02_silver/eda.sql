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

-- EXPENSIVE QUERY
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
    wind_speed_value::FLOAT as wind_speed_10m_kmh,
    cloud_cover::FLOAT as cloud_cover_percent,
    uv_index::FLOAT as uv_index,
    rain::FLOAT as rain_mm,
    showers::FLOAT as showers_mm,
    snowfall::FLOAT as snowfall_mm
FROM bronze.weather_raw,
     UNNEST(
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'time')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'temperature_2m')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'apparent_temperature')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'relative_humidity_2m')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'precipitation')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'weather_code')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'is_day')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'wind_speed_10m')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'cloud_cover')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'rain')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'showers')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'snowfall')),
         ARRAY(SELECT jsonb_array_elements_text(raw_api_response -> 'hourly' -> 'uv_index'))
     ) AS t(time_value, temp_value, apparent_temp_value, humidity_value, precipitation_value, weather_code_value, is_day_value, wind_speed_value, cloud_cover, rain, showers, snowfall, uv_index)
;

-- OPTIMAL
SELECT
    w.id as bronze_record_id,
    (w.raw_api_response->>'latitude')::FLOAT as latitude,
    (w.raw_api_response->>'longitude')::FLOAT as longitude,
    w.raw_api_response->>'timezone' as timezone,  -- ✅ Fixed: added timezone
    h.time_value::TIMESTAMPTZ as observation_timestamp,
    h.temp_value::FLOAT as temperature_2m_celsius,
    h.apparent_temp_value::FLOAT as apparent_temperature_celsius,
    h.humidity_value::FLOAT as relative_humidity_2m_percent,
    h.precipitation_value::FLOAT as precipitation_mm,
    h.weather_code_value as weather_code,
    h.is_day_value::BOOLEAN as is_day,
    h.wind_speed_value::FLOAT as wind_speed_10m_kmh,
    h.cloud_cover_value::FLOAT as cloud_cover_percent,
    h.uv_index_value::FLOAT as uv_index,
    h.rain_value::FLOAT as rain_mm,
    h.showers_value::FLOAT as showers_mm,
    h.snowfall_value::FLOAT as snowfall_mm,
    w.created_at as api_retrieval_time  -- ✅ Fixed: added api_retrieval_time
FROM bronze.weather_raw w
CROSS JOIN LATERAL (
    SELECT 
        time_elem->>0 as time_value,
        temp_elem->>0 as temp_value,
        apparent_temp_elem->>0 as apparent_temp_value,
        humidity_elem->>0 as humidity_value,
        precipitation_elem->>0 as precipitation_value,
        weather_code_elem->>0 as weather_code_value,
        is_day_elem->>0 as is_day_value,
        wind_speed_elem->>0 as wind_speed_value,
        cloud_cover_elem->>0 as cloud_cover_value,
        rain_elem->>0 as rain_value,
        showers_elem->>0 as showers_value,
        snowfall_elem->>0 as snowfall_value,
        uv_index_elem->>0 as uv_index_value,
        row_number() OVER () as rn
    FROM 
        jsonb_array_elements(w.raw_api_response->'hourly'->'time') WITH ORDINALITY AS time_elem(val, time_idx)
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'temperature_2m') WITH ORDINALITY AS temp_elem(val, temp_idx) 
        ON time_elem.time_idx = temp_elem.temp_idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'apparent_temperature') WITH ORDINALITY AS apparent_temp_elem(val, idx) 
        ON time_elem.time_idx = apparent_temp_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'relative_humidity_2m') WITH ORDINALITY AS humidity_elem(val, idx) 
        ON time_elem.time_idx = humidity_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'precipitation') WITH ORDINALITY AS precipitation_elem(val, idx) 
        ON time_elem.time_idx = precipitation_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'weather_code') WITH ORDINALITY AS weather_code_elem(val, idx) 
        ON time_elem.time_idx = weather_code_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'is_day') WITH ORDINALITY AS is_day_elem(val, idx) 
        ON time_elem.time_idx = is_day_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'wind_speed_10m') WITH ORDINALITY AS wind_speed_elem(val, idx) 
        ON time_elem.time_idx = wind_speed_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'cloud_cover') WITH ORDINALITY AS cloud_cover_elem(val, idx) 
        ON time_elem.time_idx = cloud_cover_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'rain') WITH ORDINALITY AS rain_elem(val, idx) 
        ON time_elem.time_idx = rain_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'showers') WITH ORDINALITY AS showers_elem(val, idx) 
        ON time_elem.time_idx = showers_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'snowfall') WITH ORDINALITY AS snowfall_elem(val, idx) 
        ON time_elem.time_idx = snowfall_elem.idx
    JOIN jsonb_array_elements(w.raw_api_response->'hourly'->'uv_index') WITH ORDINALITY AS uv_index_elem(val, idx) 
        ON time_elem.time_idx = uv_index_elem.idx
) h
WHERE w.created_at > NOW() - INTERVAL '1 hour'  -- Only process recent data
ON CONFLICT (latitude, longitude, observation_timestamp) 
DO NOTHING;  -- Skip duplicates