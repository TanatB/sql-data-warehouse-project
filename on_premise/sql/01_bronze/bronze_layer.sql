CREATE SCHEMA IF NOT EXISTS bronze;

-- TABLE
CREATE TABLE IF NOT EXISTS bronze.weather_raw (
    record_id SERIAL PRIMARY KEY,

    location_name VARCHAR(100),
    latitude NUMERIC(8, 4) NOT NULL,
    longitude NUMERIC(8, 4) NOT NULL,

    -- retrieval_time is recorded When API was called
    api_retrieval_time TIMESTAMPTZ NOT NULL,
    timezone VARCHAR(50),
    response_time_ms INTEGER,

    -- AUDIT, when record was created, for future batch processing
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- API RESPONSE
    raw_api_response JSONB NOT NULL,

    CONSTRAINT unique_location_request
        UNIQUE (location_name, api_retrieval_time)
);


-- For insertion failure tracking
CREATE TABLE IF NOT EXISTS bronze.api_error_log (
    error_id SERIAL PRIMARY KEY,
    error_timestamp TIMESTAMP DEFAULT NOW(),
    api_endpoint VARCHAR(500),
    error_type VARCHAR(100),
    error_message TEXT,
    response_status_code INTEGER,
    request_params JSONB
);


-- INDEX
CREATE INDEX idx_bronze_location
    ON bronze.weather_raw(location_name);
CREATE INDEX idx_bronze_created_at
    ON bronze.weather_raw(created_at);

CREATE INDEX idx_error_timestamp 
    ON bronze.api_error_log(error_timestamp DESC);