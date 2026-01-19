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
    relative_humidity_2m_precent FLOAT,
    precipitation_mm FLOAT,
    weather_code INTEGER,
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

-- TODO: TRANSFROMATION
INSERT INTO silver.weather_observations
    SELECT *
    FROM bronze.weather_raw