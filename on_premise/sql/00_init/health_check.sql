SELECT 
    'bronze.weather_raw' as table_name,
    COUNT(*) as row_count,
    MAX(extracted_at) as latest_data,
    NOW() - MAX(extracted_at) as data_age
FROM bronze.weather_raw;