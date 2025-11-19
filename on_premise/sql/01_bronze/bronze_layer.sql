
-- TABLE
CREATE TABLE IF NOT EXISTS bronze.weather_raw (
    record_id SERIAL PRIMARY KEY,

    -- retrieval_time is recorded When API was called
    api_retrieval_time TIMESTAMP NOT NULL,
    response_time_ms INTEGER,

    -- AUDIT, when record was created, for future batch processing
    created_at TIMESTAMP DEFAULT NOW(),

    -- API RESPONSE
    raw_api_response JSONB NOT NULL
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
CREATE INDEX idx_weather_raw_record_id
    ON bronze.weather_raw (record_id);
CREATE INDEX idx_weather_raw_api_retrieval_time
    ON bronze.weather_raw (api_retrieval_time);

CREATE INDEX idx_error_timestamp 
    ON bronze.api_error_log(error_timestamp DESC);