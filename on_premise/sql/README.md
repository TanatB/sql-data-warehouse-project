# Medallion Architecture Data Warehouse - SQL Scripts

## Overview

This directory contains comprehensive SQL scripts for building an enterprise-grade data warehouse using the **Medallion Architecture** (Bronze → Silver → Gold) on PostgreSQL 16.

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                              │
│              Business-Ready Dimensional Model                   │
│         (Star Schema, Fact & Dimension Tables)                  │
│  • Optimized for BI and Analytics                              │
│  • Pre-aggregated metrics                                      │
│  • SCD Type 2 dimensions                                       │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                             │
│              Cleaned and Validated Data                         │
│  • Business rules applied                                      │
│  • Data quality checks                                         │
│  • Conformed dimensions                                        │
│  • Calculated fields                                           │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                             │
│              Raw Data Landing Zone                              │
│  • Minimal transformation                                      │
│  • Full history retained                                       │
│  • Source system fidelity                                      │
│  • Audit trail                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
sql/
├── 00_init/
│   └── init_database.sql           # Database initialization, roles, schemas
├── 01_bronze/
│   └── ddl_bronze.sql              # Raw data tables with partitioning
├── 02_silver/
│   └── ddl_silver.sql              # Cleaned data tables with validation
├── 03_gold/
│   └── ddl_gold.sql                # Dimensional model (star schema)
└── 04_utilities/
    ├── role_management.sql         # RBAC and user management
    ├── performance_indexing.sql    # Performance optimization
    └── monitoring_health.sql       # Database monitoring and health checks
```

## Quick Start Guide

### Prerequisites

- PostgreSQL 16 (or later)
- Superuser access to PostgreSQL
- `psql` command-line tool installed

### Installation Steps

**Step 1: Initialize Database**

```bash
psql -U postgres -f 00_init/init_database.sql
```

This creates:
- Database: `warehouse_db`
- Schemas: `bronze`, `silver`, `gold`, `staging`, `audit`
- Roles: `dw_admin`, `dw_developer`, `dw_analyst`, `dw_readonly`, `airflow_service`
- Audit tables and functions

**Step 2: Create Bronze Layer**

```bash
psql -U postgres -d warehouse_db -f 01_bronze/ddl_bronze.sql
```

This creates:
- Raw data tables with time-based partitioning
- Data validation tables
- API logging tables
- Automatic validation triggers

**Step 3: Create Silver Layer**

```bash
psql -U postgres -d warehouse_db -f 02_silver/ddl_silver.sql
```

This creates:
- Cleaned data tables with constraints
- SCD Type 2 dimensions
- Transformation functions
- Data quality views

**Step 4: Create Gold Layer**

```bash
psql -U postgres -d warehouse_db -f 03_gold/ddl_gold.sql
```

This creates:
- Dimension tables (Date, Time, Location, Weather Condition)
- Fact tables (Hourly, Daily, Monthly aggregates)
- Business intelligence views
- Pre-populated date and time dimensions

**Step 5: Setup Utilities (Optional but Recommended)**

```bash
# Role management
psql -U postgres -d warehouse_db -f 04_utilities/role_management.sql

# Performance monitoring
psql -U postgres -d warehouse_db -f 04_utilities/performance_indexing.sql

# Health monitoring
psql -U postgres -d warehouse_db -f 04_utilities/monitoring_health.sql
```

## User Roles and Permissions

### Role Hierarchy

| Role | Description | Access |
|------|-------------|--------|
| `dw_admin` | Database Administrator | Full access to all schemas |
| `dw_developer` | Data Engineer/Developer | Read/Write Bronze & Silver, Read Gold |
| `etl_developer` | ETL Developer | Full Bronze & Silver, Read Gold |
| `dw_analyst` | Business Analyst | Read Silver & Gold |
| `bi_analyst` | BI Analyst | Read Gold only |
| `data_scientist` | Data Scientist | Read Silver & Gold |
| `dw_readonly` | Read-Only User | Read Gold only |
| `airflow_service` | ETL Service Account | Full access to all layers |

### Creating Users

```sql
-- Create a new developer
SELECT public.create_developer_user('john_doe', 'secure_password', 'John Doe');

-- Create a new analyst
SELECT public.create_analyst_user('jane_smith', 'secure_password', 'Jane Smith');

-- Revoke user access
SELECT public.revoke_user_access('old_user', 'Left company');
```

### Viewing Permissions

```sql
-- View all users and their roles
SELECT * FROM public.vw_user_permissions;

-- View table-level permissions
SELECT * FROM public.vw_table_permissions WHERE schema_name = 'gold';
```

## Database Schema Details

### Bronze Layer

**Purpose:** Raw data ingestion with minimal transformation

**Tables:**
- `raw_weather_data` - Partitioned by ingestion_date (monthly)
- `raw_api_logs` - API call logging
- `raw_file_ingestion_log` - File ingestion tracking
- `raw_data_validation` - Data quality validation results

**Key Features:**
- Time-based partitioning for performance
- JSONB support for flexible schema
- Automatic data validation triggers
- Full audit trail

**Partitioning:**
```sql
-- Partitions are created monthly: raw_weather_data_YYYY_MM
-- Example: raw_weather_data_2025_10, raw_weather_data_2025_11
```

### Silver Layer

**Purpose:** Cleaned, validated, and conformed data

**Tables:**
- `locations` - Location dimension (SCD Type 2)
- `weather_conditions` - Weather condition reference
- `weather_observations` - Partitioned cleaned weather data (monthly)
- `data_anomalies` - Anomaly detection tracking

**Key Features:**
- Business rule validation
- SCD Type 2 for tracking dimensional changes
- Calculated derived metrics (wind chill, dew point, etc.)
- Foreign key constraints for data integrity
- CHECK constraints for data validation

**Transformation Functions:**
```sql
-- Temperature conversions
SELECT silver.kelvin_to_celsius(298.15);  -- Returns 25.00
SELECT silver.celsius_to_fahrenheit(25);  -- Returns 77.00

-- Wind calculations
SELECT silver.degrees_to_cardinal(135);   -- Returns 'SE'
SELECT silver.wind_to_beaufort(15.5);     -- Returns Beaufort scale value

-- Dew point calculation
SELECT silver.calculate_dew_point(25, 60);  -- temp_celsius, humidity_percent
```

### Gold Layer

**Purpose:** Business-ready dimensional model for analytics

**Dimensional Model (Star Schema):**

**Dimensions:**
- `dim_date` - Date dimension (pre-populated 2023-2027)
- `dim_time` - Time dimension (hourly)
- `dim_location` - Location dimension (SCD Type 2)
- `dim_weather_condition` - Weather condition dimension

**Fact Tables:**
- `fact_weather_hourly` - Partitioned hourly observations
- `fact_weather_daily` - Daily aggregates
- `fact_weather_monthly` - Monthly statistics

**Business Views:**
- `vw_current_weather_dashboard` - Real-time weather snapshot
- `vw_daily_weather_summary` - Daily weather reports
- `vw_temperature_trends` - Monthly trends

**Example Queries:**

```sql
-- Get current weather for all locations
SELECT * FROM gold.vw_current_weather_dashboard;

-- Daily weather summary for last 7 days
SELECT *
FROM gold.vw_daily_weather_summary
WHERE full_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY full_date DESC;

-- Monthly temperature trends
SELECT
    location_name,
    year,
    month_name,
    avg_monthly_temp_celsius,
    total_monthly_precipitation_mm
FROM gold.vw_temperature_trends
WHERE year = 2025
ORDER BY year, month_number;
```

## Performance Optimization

### Indexing Strategy

The database includes comprehensive indexing for optimal performance:

**Bronze Layer Indexes:**
- Time-based indexes (ingestion_timestamp, observation_timestamp)
- Location lookup indexes
- JSON GIN indexes for flexible querying
- Data quality flag indexes

**Silver Layer Indexes:**
- Foreign key indexes
- Time-series indexes
- Composite indexes for common queries
- Partial indexes for current records (SCD Type 2)

**Gold Layer Indexes:**
- Star schema foreign key indexes
- Time dimension indexes
- Measure columns for filtering

### Monitoring Index Health

```sql
-- View index usage statistics
SELECT * FROM public.vw_index_usage_stats
WHERE usage_category = 'UNUSED';

-- Identify missing indexes on foreign keys
SELECT * FROM public.vw_missing_indexes;

-- Find duplicate indexes
SELECT * FROM public.vw_duplicate_indexes;

-- Check index bloat
SELECT * FROM public.vw_index_bloat
WHERE scans < 100 AND index_size > '1 MB';
```

### Partition Management

```sql
-- Create partitions for next 3 months
SELECT public.create_future_partitions('bronze', 'raw_weather_data', 3);
SELECT public.create_future_partitions('silver', 'weather_observations', 3);
SELECT public.create_future_partitions('gold', 'fact_weather_hourly', 3);

-- Create a specific monthly partition
SELECT public.create_monthly_partition(
    'bronze',
    'raw_weather_data',
    '2026-01-01'::DATE
);
```

### Maintenance Tasks

```sql
-- Analyze schema statistics
SELECT public.analyze_schema('bronze');
SELECT public.analyze_schema('silver');
SELECT public.analyze_schema('gold');

-- Vacuum tables with high dead tuple percentage (> 10%)
SELECT * FROM public.vacuum_bloated_tables(10.0);

-- Reindex a specific table
SELECT public.reindex_table('silver', 'weather_observations');
```

## Database Health Monitoring

### Health Check Dashboard

```sql
-- Comprehensive health check
SELECT * FROM public.health_check();

-- Expected output:
--  check_name               | status | value      | threshold  | message
-- --------------------------+--------+------------+------------+----------------------------
--  Cache Hit Ratio          | PASS   | 98.5%      | > 95%      | Cache hit ratio is healthy
--  Connection Usage         | PASS   | 15 / 100   | < 80% max  | Connection usage is healthy
--  Dead Tuple Percentage    | PASS   | 3.2%       | < 10%      | Dead tuple percentage is healthy
--  Long Running Queries     | PASS   | 0          | 0          | No long running queries detected
--  Blocking Queries         | PASS   | 0          | 0          | No blocking queries detected
```

### Connection Monitoring

```sql
-- View active connections
SELECT * FROM public.vw_active_connections;

-- Connection summary by state
SELECT * FROM public.vw_connection_summary;

-- Kill idle connections (idle > 30 minutes)
SELECT * FROM public.kill_idle_connections('30 minutes');
```

### Lock Monitoring

```sql
-- View current locks
SELECT * FROM public.vw_current_locks
WHERE NOT granted;

-- Identify blocking queries
SELECT * FROM public.vw_blocking_queries;
```

### Size and Growth Monitoring

```sql
-- Total database size
SELECT * FROM public.vw_database_size;

-- Size by schema
SELECT * FROM public.vw_schema_sizes;

-- Largest tables
SELECT * FROM public.vw_largest_tables LIMIT 10;

-- Table statistics with dead tuples
SELECT *
FROM public.vw_table_statistics
WHERE dead_tuple_percent > 10
ORDER BY dead_tuple_percent DESC;
```

### Cache Performance

```sql
-- Cache hit ratios (should be > 95%)
SELECT * FROM public.vw_cache_hit_ratios;

-- Transaction statistics
SELECT * FROM public.vw_transaction_stats;
```

## Data Quality and Audit

### ETL Execution Tracking

```sql
-- Log ETL job start
SELECT audit.log_etl_start(
    'weather_ingestion',
    'bronze',
    'raw_weather_data',
    '{"source": "OpenWeatherAPI", "batch_id": "20251015_001"}'::jsonb
);

-- Log ETL job completion
SELECT audit.log_etl_complete(
    '<execution_id>',  -- UUID returned from log_etl_start
    'SUCCESS',
    1500,  -- rows_inserted
    0,     -- rows_updated
    0,     -- rows_deleted
    NULL   -- error_message
);

-- View recent ETL executions
SELECT *
FROM audit.etl_execution_log
WHERE execution_start_time >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY execution_start_time DESC;
```

### Data Quality Monitoring

```sql
-- View data quality checks
SELECT *
FROM audit.data_quality_log
WHERE check_timestamp >= CURRENT_DATE - INTERVAL '1 day'
    AND check_passed = FALSE
ORDER BY check_timestamp DESC;

-- Bronze layer quality summary
SELECT * FROM bronze.vw_data_quality_summary
WHERE ingestion_date >= CURRENT_DATE - INTERVAL '7 days';

-- Silver layer anomalies
SELECT *
FROM silver.data_anomalies
WHERE detected_timestamp >= CURRENT_DATE - INTERVAL '1 day'
    AND is_resolved = FALSE;
```

### Data Lineage

```sql
-- View data lineage
SELECT *
FROM audit.table_lineage
WHERE is_active = TRUE
ORDER BY source_schema, source_table;

-- Add lineage tracking
INSERT INTO audit.table_lineage (
    source_schema, source_table,
    target_schema, target_table,
    transformation_type, lineage_description
) VALUES (
    'bronze', 'raw_weather_data',
    'silver', 'weather_observations',
    'TRANSFORM', 'Clean and validate raw weather data with business rules'
);
```

## Recommended Maintenance Schedule

### Daily (Off-Peak Hours - 2:00 AM)

```sql
-- Update table statistics
SELECT public.analyze_schema('bronze');
SELECT public.analyze_schema('silver');
SELECT public.analyze_schema('gold');

-- Create future partitions
SELECT public.create_future_partitions('bronze', 'raw_weather_data', 2);
SELECT public.create_future_partitions('silver', 'weather_observations', 2);
SELECT public.create_future_partitions('gold', 'fact_weather_hourly', 2);

-- Health check
SELECT * FROM public.health_check();
```

### Weekly (Weekend - 3:00 AM)

```sql
-- Check index usage
SELECT * FROM public.vw_index_usage_stats WHERE usage_category = 'UNUSED';

-- Vacuum tables with high dead tuples
SELECT * FROM public.vacuum_bloated_tables(10.0);

-- Review slow queries
SELECT * FROM public.vw_slow_queries LIMIT 10;

-- Kill idle connections
SELECT * FROM public.kill_idle_connections('1 hour');
```

### Monthly (First Sunday - 4:00 AM)

```sql
-- Full vacuum (requires maintenance window)
VACUUM FULL ANALYZE bronze.raw_api_logs;
VACUUM FULL ANALYZE audit.etl_execution_log;

-- Reindex large tables
SELECT public.reindex_table('silver', 'weather_observations');
SELECT public.reindex_table('gold', 'fact_weather_hourly');

-- Review and document schema changes
-- Archive old partitions (manual decision)
```

### Quarterly

- Review table partitioning strategy
- Evaluate index effectiveness
- Database size and growth projection
- Performance tuning and optimization review
- Security audit and user access review

## Troubleshooting

### Common Issues

**Issue: High dead tuple percentage**

```sql
-- Check affected tables
SELECT * FROM public.vw_table_statistics
WHERE dead_tuple_percent > 10;

-- Solution: Run VACUUM
SELECT * FROM public.vacuum_bloated_tables(5.0);
```

**Issue: Slow queries**

```sql
-- Identify slow queries
SELECT * FROM public.vw_slow_queries LIMIT 10;

-- Check missing indexes
SELECT * FROM public.vw_missing_indexes;

-- Analyze table statistics
ANALYZE bronze.raw_weather_data;
```

**Issue: Connection pool exhaustion**

```sql
-- Check active connections
SELECT * FROM public.vw_connection_summary;

-- Kill idle connections
SELECT * FROM public.kill_idle_connections('30 minutes');
```

**Issue: Partition not found**

```sql
-- Create missing partition
SELECT public.create_monthly_partition(
    'bronze',
    'raw_weather_data',
    CURRENT_DATE
);
```

## Security Best Practices

1. **Change Default Passwords:** Update all passwords in `00_init/init_database.sql` before deployment
2. **Use SSL Connections:** Configure PostgreSQL to require SSL for all connections
3. **Regular Password Rotation:** Implement password rotation policy for all service accounts
4. **Audit User Access:** Regularly review `vw_user_permissions` and `vw_table_permissions`
5. **Row-Level Security:** Enabled on sensitive audit tables
6. **Least Privilege:** Grant minimum necessary permissions for each role

## Integration with BI Tools

### Power BI / Tableau / Looker

Connect to the Gold layer for business intelligence:

```
Host: postgres_warehouse
Port: 5433
Database: warehouse_db
Schema: gold
User: bi_analyst
```

**Recommended Views:**
- `gold.vw_current_weather_dashboard`
- `gold.vw_daily_weather_summary`
- `gold.vw_temperature_trends`

### DBT (Data Build Tool)

The Silver and Gold layers are designed for DBT integration:

```yaml
# profiles.yml
warehouse_db:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres_warehouse
      port: 5433
      user: dw_developer
      password: "{{ env_var('DBT_PASSWORD') }}"
      dbname: warehouse_db
      schema: gold
```

## Additional Resources

- [PostgreSQL Partitioning Documentation](https://www.postgresql.org/docs/16/ddl-partitioning.html)
- [Slowly Changing Dimensions (SCD)](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## Support and Contribution

For issues, questions, or contributions:
1. Review this README thoroughly
2. Check existing issues and documentation
3. Create detailed issue reports with reproduction steps
4. Follow SQL coding standards and naming conventions

## License

See [LICENSE](../LICENSE) file for details.

---

**Last Updated:** October 2025
**Database Version:** PostgreSQL 16
**Architecture Version:** 1.0
