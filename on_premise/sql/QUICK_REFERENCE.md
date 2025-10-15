# Quick Reference Guide - Medallion Data Warehouse

## Installation (5 Minutes)

```bash
# 1. Initialize database
psql -U postgres -f 00_init/init_database.sql

# 2. Create all layers
psql -U postgres -d warehouse_db -f 01_bronze/ddl_bronze.sql
psql -U postgres -d warehouse_db -f 02_silver/ddl_silver.sql
psql -U postgres -d warehouse_db -f 03_gold/ddl_gold.sql

# 3. Setup utilities
psql -U postgres -d warehouse_db -f 04_utilities/role_management.sql
psql -U postgres -d warehouse_db -f 04_utilities/performance_indexing.sql
psql -U postgres -d warehouse_db -f 04_utilities/monitoring_health.sql
```

## Essential Commands

### User Management

```sql
-- Create developer
SELECT create_developer_user('username', 'password', 'Full Name');

-- Create analyst
SELECT create_analyst_user('username', 'password', 'Full Name');

-- Revoke access
SELECT revoke_user_access('username', 'reason');

-- View all users
SELECT * FROM vw_user_permissions;
```

### Health Monitoring

```sql
-- Quick health check
SELECT * FROM health_check();

-- Active connections
SELECT * FROM vw_active_connections;

-- Database size
SELECT * FROM vw_database_size;

-- Schema sizes
SELECT * FROM vw_schema_sizes;

-- Largest tables
SELECT * FROM vw_largest_tables LIMIT 10;

-- Cache hit ratio (should be > 95%)
SELECT * FROM vw_cache_hit_ratios;
```

### Performance

```sql
-- Unused indexes
SELECT * FROM vw_index_usage_stats WHERE usage_category = 'UNUSED';

-- Missing indexes
SELECT * FROM vw_missing_indexes;

-- Slow queries
SELECT * FROM vw_slow_queries LIMIT 10;

-- Analyze schemas
SELECT analyze_schema('bronze');
SELECT analyze_schema('silver');
SELECT analyze_schema('gold');

-- Vacuum bloated tables
SELECT * FROM vacuum_bloated_tables(10.0);
```

### Partition Management

```sql
-- Create future partitions (next 3 months)
SELECT create_future_partitions('bronze', 'raw_weather_data', 3);
SELECT create_future_partitions('silver', 'weather_observations', 3);
SELECT create_future_partitions('gold', 'fact_weather_hourly', 3);

-- Create specific partition
SELECT create_monthly_partition('bronze', 'raw_weather_data', '2026-01-01'::DATE);
```

### ETL Logging

```sql
-- Log ETL start
SELECT audit.log_etl_start('job_name', 'schema_name', 'table_name', '{"key": "value"}'::jsonb);

-- Log ETL completion
SELECT audit.log_etl_complete(
    '<execution_id>',  -- UUID from log_etl_start
    'SUCCESS',         -- or 'FAILED'
    1000,              -- rows_inserted
    0,                 -- rows_updated
    0,                 -- rows_deleted
    NULL               -- error_message
);

-- View recent executions
SELECT * FROM audit.etl_execution_log
WHERE execution_start_time >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY execution_start_time DESC;
```

### Data Quality

```sql
-- Bronze quality summary
SELECT * FROM bronze.vw_data_quality_summary;

-- Silver anomalies
SELECT * FROM silver.data_anomalies
WHERE is_resolved = FALSE
ORDER BY detected_timestamp DESC;

-- Quality checks
SELECT * FROM audit.data_quality_log
WHERE check_passed = FALSE
ORDER BY check_timestamp DESC
LIMIT 20;
```

### Business Queries (Gold Layer)

```sql
-- Current weather dashboard
SELECT * FROM gold.vw_current_weather_dashboard;

-- Last 7 days daily summary
SELECT * FROM gold.vw_daily_weather_summary
WHERE full_date >= CURRENT_DATE - INTERVAL '7 days';

-- Monthly temperature trends
SELECT * FROM gold.vw_temperature_trends
WHERE year = 2025;

-- Hourly weather for specific location
SELECT
    d.full_date,
    t.hour_name,
    f.temperature_celsius,
    f.humidity_percent,
    f.wind_speed_kmh,
    wc.condition_description
FROM gold.fact_weather_hourly f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_time t ON f.time_key = t.time_key
JOIN gold.dim_location l ON f.location_key = l.location_key
LEFT JOIN gold.dim_weather_condition wc ON f.condition_key = wc.condition_key
WHERE l.location_name = 'New York'
    AND d.full_date = CURRENT_DATE
ORDER BY t.hour;
```

## Roles Quick Reference

| Role | Bronze | Silver | Gold | Audit | Use Case |
|------|--------|--------|------|-------|----------|
| dw_admin | Full | Full | Full | Full | Admin |
| dw_developer | R/W | R/W | Read | Read | Data Engineer |
| etl_developer | R/W | R/W | Read | Read | ETL Dev |
| dw_analyst | - | Read | Read | - | Business Analyst |
| bi_analyst | - | - | Read | - | BI Developer |
| data_scientist | - | Read | Read | - | Data Scientist |
| dw_readonly | - | - | Read | - | Report Viewer |
| airflow_service | Full | Full | Full | Full | ETL Service |

## Connection Strings

### PostgreSQL CLI
```bash
# Admin
psql -h postgres_warehouse -p 5433 -U dw_admin -d warehouse_db

# Developer
psql -h postgres_warehouse -p 5433 -U dw_developer -d warehouse_db

# Analyst
psql -h postgres_warehouse -p 5433 -U dw_analyst -d warehouse_db
```

### Python (psycopg2)
```python
import psycopg2

conn = psycopg2.connect(
    host="postgres_warehouse",
    port=5433,
    database="warehouse_db",
    user="dw_developer",
    password="your_password"
)
```

### SQLAlchemy
```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://dw_developer:password@postgres_warehouse:5433/warehouse_db"
)
```

## Daily Maintenance Checklist

- [ ] Run `SELECT * FROM health_check();`
- [ ] Check `SELECT * FROM vw_cache_hit_ratios;` (should be > 95%)
- [ ] Verify `SELECT * FROM vw_active_connections;`
- [ ] Create future partitions if needed
- [ ] Review ETL execution logs for errors
- [ ] Check for data quality issues

## Weekly Maintenance Checklist

- [ ] Identify and remove unused indexes
- [ ] Run `SELECT * FROM vacuum_bloated_tables(10.0);`
- [ ] Review slow queries
- [ ] Check for blocking queries
- [ ] Kill idle connections
- [ ] Analyze all schemas

## Emergency Commands

### Kill Blocking Query
```sql
-- Find blocking query
SELECT * FROM vw_blocking_queries;

-- Kill it (use blocking_pid from above)
SELECT pg_terminate_backend(<blocking_pid>);
```

### Kill All Idle Connections
```sql
SELECT * FROM kill_idle_connections('30 minutes');
```

### Emergency Vacuum
```sql
VACUUM FULL ANALYZE schema_name.table_name;
```

### Force Reindex
```sql
SELECT reindex_table('schema_name', 'table_name');
-- or
REINDEX TABLE CONCURRENTLY schema_name.table_name;
```

## Troubleshooting

### Problem: Out of Memory
**Check:** `SELECT * FROM vw_cache_hit_ratios;`
**Solution:** Increase `shared_buffers` or add more RAM

### Problem: Connection Limit Reached
**Check:** `SELECT * FROM vw_connection_summary;`
**Solution:** `SELECT * FROM kill_idle_connections('15 minutes');`

### Problem: Slow Queries
**Check:** `SELECT * FROM vw_slow_queries LIMIT 10;`
**Solution:** Add missing indexes or run ANALYZE

### Problem: High Dead Tuples
**Check:** `SELECT * FROM vw_table_statistics WHERE dead_tuple_percent > 10;`
**Solution:** `SELECT * FROM vacuum_bloated_tables(5.0);`

### Problem: Partition Not Found
**Check:** Query date range
**Solution:** `SELECT create_monthly_partition('schema', 'table', '2026-01-01'::DATE);`

## Important File Locations

```
sql/
├── 00_init/init_database.sql          # Start here
├── 01_bronze/ddl_bronze.sql           # Raw data layer
├── 02_silver/ddl_silver.sql           # Cleaned data layer
├── 03_gold/ddl_gold.sql               # Analytics layer
└── 04_utilities/
    ├── role_management.sql            # User mgmt
    ├── performance_indexing.sql       # Performance
    └── monitoring_health.sql          # Monitoring
```

## Support

1. Check [README.md](README.md) for detailed documentation
2. Review script comments for inline documentation
3. Check PostgreSQL logs for errors
4. Run health_check() for diagnostics

---

**Quick Help:**
- Installation issues → Check 00_init/init_database.sql
- Performance issues → Check 04_utilities/performance_indexing.sql
- User access issues → Check 04_utilities/role_management.sql
- Monitoring issues → Check 04_utilities/monitoring_health.sql
