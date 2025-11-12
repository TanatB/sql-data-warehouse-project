
-- TABLE
CREATE TABLE IF NOT EXISTS bronze.weather_raw(
    record_id SERIAL PRIMARY KEY,
    
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    api_retrieval_time TIMESTAMP NOT NULL,
    request_id INTEGER,

    -- API RESPONSE
    raw_response JSONB,

    -- METADATA
    response_time_ms INTEGER,
    forecast_days INTEGER,
    
    -- AUDIT
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- INDEX
CREATE INDEX idx_forecast_location;
CREATE INDEX idx_forecast_time;