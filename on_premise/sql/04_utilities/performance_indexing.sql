-- ============================================================================
-- Performance Optimization and Indexing Strategy
-- ============================================================================
-- Purpose: Monitor and optimize database performance through indexing
-- Usage: Run periodically to analyze and maintain indexes
-- ============================================================================

\c warehouse_db

-- ============================================================================
-- SECTION 1: INDEX MONITORING VIEWS
-- ============================================================================

-- View: Index usage statistics
CREATE OR REPLACE VIEW public.vw_index_usage_stats AS
SELECT
    schemaname as schema_name,
    tablename as table_name,
    indexname as index_name,
    idx_scan as number_of_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    CASE
        WHEN idx_scan = 0 THEN 'UNUSED'
        WHEN idx_scan < 100 THEN 'LOW_USAGE'
        WHEN idx_scan < 1000 THEN 'MODERATE_USAGE'
        ELSE 'HIGH_USAGE'
    END as usage_category
FROM pg_stat_user_indexes
WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit')
ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC;

COMMENT ON VIEW public.vw_index_usage_stats IS 'Index usage statistics to identify unused or underutilized indexes';

-- View: Missing indexes (tables without indexes on foreign keys)
CREATE OR REPLACE VIEW public.vw_missing_indexes AS
SELECT
    n.nspname as schema_name,
    t.relname as table_name,
    a.attname as column_name,
    pg_size_pretty(pg_total_relation_size(t.oid)) as table_size
FROM pg_catalog.pg_constraint c
JOIN pg_catalog.pg_class t ON c.conrelid = t.oid
JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
JOIN pg_catalog.pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = t.oid
WHERE c.contype = 'f'  -- Foreign key constraints
    AND n.nspname IN ('bronze', 'silver', 'gold')
    AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_index i
        WHERE i.indrelid = t.oid
            AND a.attnum = ANY(i.indkey)
    )
ORDER BY pg_total_relation_size(t.oid) DESC;

COMMENT ON VIEW public.vw_missing_indexes IS 'Foreign key columns without indexes (performance risk)';

-- View: Duplicate indexes
CREATE OR REPLACE VIEW public.vw_duplicate_indexes AS
SELECT
    n.nspname as schema_name,
    t.relname as table_name,
    STRING_AGG(i.relname, ', ' ORDER BY i.relname) as duplicate_indexes,
    pg_get_indexdef(idx.indexrelid) as index_definition,
    pg_size_pretty(SUM(pg_relation_size(idx.indexrelid))) as total_size
FROM pg_catalog.pg_index idx
JOIN pg_catalog.pg_class i ON i.oid = idx.indexrelid
JOIN pg_catalog.pg_class t ON t.oid = idx.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname IN ('bronze', 'silver', 'gold', 'audit')
GROUP BY n.nspname, t.relname, idx.indkey::text, idx.indclass::text, idx.indoption::text
HAVING COUNT(*) > 1
ORDER BY SUM(pg_relation_size(idx.indexrelid)) DESC;

COMMENT ON VIEW public.vw_duplicate_indexes IS 'Identifies duplicate indexes wasting disk space';

-- View: Index bloat estimation
CREATE OR REPLACE VIEW public.vw_index_bloat AS
SELECT
    schemaname as schema_name,
    tablename as table_name,
    indexname as index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan as scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    CASE
        WHEN idx_tup_read > 0 THEN
            ROUND(100.0 * idx_tup_fetch / idx_tup_read, 2)
        ELSE 0
    END as fetch_ratio_percent
FROM pg_stat_user_indexes
WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit')
    AND pg_relation_size(indexrelid) > 1024 * 1024  -- Larger than 1MB
ORDER BY pg_relation_size(indexrelid) DESC;

COMMENT ON VIEW public.vw_index_bloat IS 'Estimates index bloat and efficiency';

-- ============================================================================
-- SECTION 2: TABLE STATISTICS VIEWS
-- ============================================================================

-- View: Table size and statistics
CREATE OR REPLACE VIEW public.vw_table_statistics AS
SELECT
    schemaname as schema_name,
    tablename as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) -
                   pg_relation_size(schemaname||'.'||tablename)) as indexes_size,
    n_live_tup as live_tuples,
    n_dead_tup as dead_tuples,
    CASE
        WHEN n_live_tup > 0 THEN
            ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
        ELSE 0
    END as dead_tuple_percent,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    seq_scan as sequential_scans,
    idx_scan as index_scans,
    CASE
        WHEN (seq_scan + idx_scan) > 0 THEN
            ROUND(100.0 * idx_scan / (seq_scan + idx_scan), 2)
        ELSE 0
    END as index_usage_percent
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

COMMENT ON VIEW public.vw_table_statistics IS 'Comprehensive table statistics for performance monitoring';

-- ============================================================================
-- SECTION 3: QUERY PERFORMANCE VIEWS
-- ============================================================================

-- View: Slow queries
CREATE OR REPLACE VIEW public.vw_slow_queries AS
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time,
    min_exec_time,
    stddev_exec_time,
    rows,
    ROUND(100.0 * shared_blks_hit / NULLIF(shared_blks_hit + shared_blks_read, 0), 2) as cache_hit_percent
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_stat_statements%'
    AND query NOT LIKE '%pg_catalog%'
ORDER BY mean_exec_time DESC
LIMIT 50;

COMMENT ON VIEW public.vw_slow_queries IS 'Top 50 slowest queries by mean execution time (requires pg_stat_statements)';

-- ============================================================================
-- SECTION 4: PARTITION MANAGEMENT FUNCTIONS
-- ============================================================================

-- Function to create monthly partitions
CREATE OR REPLACE FUNCTION public.create_monthly_partition(
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_partition_date DATE
)
RETURNS TEXT AS $$
DECLARE
    v_partition_name VARCHAR;
    v_start_date DATE;
    v_end_date DATE;
    v_sql TEXT;
BEGIN
    -- Calculate partition boundaries
    v_start_date := DATE_TRUNC('month', p_partition_date)::DATE;
    v_end_date := (DATE_TRUNC('month', p_partition_date) + INTERVAL '1 month')::DATE;

    -- Generate partition name (e.g., table_name_2025_10)
    v_partition_name := p_table_name || '_' || TO_CHAR(p_partition_date, 'YYYY_MM');

    -- Check if partition already exists
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema_name
            AND c.relname = v_partition_name
    ) THEN
        RETURN 'INFO: Partition ' || p_schema_name || '.' || v_partition_name || ' already exists';
    END IF;

    -- Create partition
    v_sql := format(
        'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
        p_schema_name,
        v_partition_name,
        p_schema_name,
        p_table_name,
        v_start_date,
        v_end_date
    );

    EXECUTE v_sql;

    RETURN 'SUCCESS: Created partition ' || p_schema_name || '.' || v_partition_name ||
           ' for range ' || v_start_date || ' to ' || v_end_date;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.create_monthly_partition IS 'Creates a monthly partition for a partitioned table';

-- Function to create next N months of partitions
CREATE OR REPLACE FUNCTION public.create_future_partitions(
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_months_ahead INTEGER DEFAULT 3
)
RETURNS TEXT[] AS $$
DECLARE
    v_results TEXT[] := ARRAY[]::TEXT[];
    v_result TEXT;
    v_current_date DATE := CURRENT_DATE;
    i INTEGER;
BEGIN
    FOR i IN 0..p_months_ahead LOOP
        SELECT public.create_monthly_partition(
            p_schema_name,
            p_table_name,
            v_current_date + (i || ' months')::INTERVAL
        ) INTO v_result;

        v_results := array_append(v_results, v_result);
    END LOOP;

    RETURN v_results;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.create_future_partitions IS 'Creates partitions for the next N months';

-- ============================================================================
-- SECTION 5: INDEX MAINTENANCE FUNCTIONS
-- ============================================================================

-- Function to reindex a specific table
CREATE OR REPLACE FUNCTION public.reindex_table(
    p_schema_name VARCHAR,
    p_table_name VARCHAR
)
RETURNS TEXT AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
BEGIN
    v_start_time := clock_timestamp();

    EXECUTE format('REINDEX TABLE CONCURRENTLY %I.%I', p_schema_name, p_table_name);

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;

    RETURN format('SUCCESS: Reindexed %s.%s in %s', p_schema_name, p_table_name, v_duration);
EXCEPTION
    WHEN OTHERS THEN
        RETURN format('ERROR: Failed to reindex %s.%s - %s', p_schema_name, p_table_name, SQLERRM);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.reindex_table IS 'Reindexes all indexes on a specific table concurrently';

-- Function to analyze tables
CREATE OR REPLACE FUNCTION public.analyze_schema(
    p_schema_name VARCHAR
)
RETURNS TEXT[] AS $$
DECLARE
    v_table RECORD;
    v_results TEXT[] := ARRAY[]::TEXT[];
BEGIN
    FOR v_table IN
        SELECT schemaname, tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = p_schema_name
    LOOP
        EXECUTE format('ANALYZE %I.%I', v_table.schemaname, v_table.tablename);
        v_results := array_append(v_results,
                                  format('Analyzed: %s.%s', v_table.schemaname, v_table.tablename));
    END LOOP;

    RETURN v_results;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.analyze_schema IS 'Runs ANALYZE on all tables in a schema to update statistics';

-- ============================================================================
-- SECTION 6: VACUUM MAINTENANCE FUNCTIONS
-- ============================================================================

-- Function to vacuum tables with high dead tuple percentage
CREATE OR REPLACE FUNCTION public.vacuum_bloated_tables(
    p_dead_tuple_threshold DECIMAL DEFAULT 10.0
)
RETURNS TABLE(
    schema_name VARCHAR,
    table_name VARCHAR,
    dead_tuple_percent DECIMAL,
    status TEXT
) AS $$
DECLARE
    v_table RECORD;
BEGIN
    FOR v_table IN
        SELECT
            schemaname,
            tablename,
            CASE
                WHEN n_live_tup > 0 THEN
                    ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                ELSE 0
            END as dead_pct
        FROM pg_stat_user_tables
        WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit')
            AND n_live_tup > 0
            AND (100.0 * n_dead_tup / (n_live_tup + n_dead_tup)) > p_dead_tuple_threshold
        ORDER BY dead_pct DESC
    LOOP
        BEGIN
            EXECUTE format('VACUUM (ANALYZE, VERBOSE) %I.%I', v_table.schemaname, v_table.tablename);

            schema_name := v_table.schemaname;
            table_name := v_table.tablename;
            dead_tuple_percent := v_table.dead_pct;
            status := 'SUCCESS';
            RETURN NEXT;
        EXCEPTION
            WHEN OTHERS THEN
                schema_name := v_table.schemaname;
                table_name := v_table.tablename;
                dead_tuple_percent := v_table.dead_pct;
                status := 'ERROR: ' || SQLERRM;
                RETURN NEXT;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.vacuum_bloated_tables IS 'Vacuums tables exceeding dead tuple threshold';

-- ============================================================================
-- SECTION 7: RECOMMENDED MAINTENANCE SCHEDULE
-- ============================================================================

/*
MAINTENANCE SCHEDULE RECOMMENDATIONS:

1. DAILY (Off-peak hours):
   - Run ANALYZE on all schemas
   - Check for missing partitions (create if needed)
   - Monitor slow queries

   Example:
   SELECT public.analyze_schema('bronze');
   SELECT public.analyze_schema('silver');
   SELECT public.analyze_schema('gold');
   SELECT public.create_future_partitions('bronze', 'raw_weather_data', 2);
   SELECT public.create_future_partitions('silver', 'weather_observations', 2);
   SELECT public.create_future_partitions('gold', 'fact_weather_hourly', 2);

2. WEEKLY (Weekend, off-peak):
   - Check index usage and remove unused indexes
   - VACUUM tables with high dead tuple percentage
   - Review and optimize slow queries

   Example:
   SELECT * FROM public.vw_index_usage_stats WHERE usage_category = 'UNUSED';
   SELECT * FROM public.vacuum_bloated_tables(10.0);
   SELECT * FROM public.vw_slow_queries LIMIT 10;

3. MONTHLY:
   - Full VACUUM on all tables
   - Reindex tables with significant bloat
   - Review and update statistics
   - Archive old partitions

   Example:
   VACUUM FULL ANALYZE bronze.raw_api_logs;
   SELECT public.reindex_table('silver', 'weather_observations');

4. QUARTERLY:
   - Review table partitioning strategy
   - Evaluate index effectiveness
   - Database size and growth analysis
   - Performance tuning review

   Example:
   SELECT * FROM public.vw_table_statistics;
   SELECT * FROM public.vw_duplicate_indexes;
   SELECT * FROM public.vw_missing_indexes;
*/

-- ============================================================================
-- SECTION 8: GRANT PERMISSIONS
-- ============================================================================

-- Grant view access to developers and analysts
GRANT SELECT ON public.vw_index_usage_stats TO dw_developer, dw_analyst;
GRANT SELECT ON public.vw_missing_indexes TO dw_developer;
GRANT SELECT ON public.vw_duplicate_indexes TO dw_developer;
GRANT SELECT ON public.vw_index_bloat TO dw_developer;
GRANT SELECT ON public.vw_table_statistics TO dw_developer, dw_analyst;

-- Grant function execution to admins and ETL service
GRANT EXECUTE ON FUNCTION public.create_monthly_partition TO dw_admin, airflow_service;
GRANT EXECUTE ON FUNCTION public.create_future_partitions TO dw_admin, airflow_service;
GRANT EXECUTE ON FUNCTION public.reindex_table TO dw_admin;
GRANT EXECUTE ON FUNCTION public.analyze_schema TO dw_admin, airflow_service;
GRANT EXECUTE ON FUNCTION public.vacuum_bloated_tables TO dw_admin;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Performance Indexing Script completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Monitoring Views:';
    RAISE NOTICE '  - vw_index_usage_stats (identify unused indexes)';
    RAISE NOTICE '  - vw_missing_indexes (FK columns without indexes)';
    RAISE NOTICE '  - vw_duplicate_indexes (redundant indexes)';
    RAISE NOTICE '  - vw_index_bloat (index efficiency)';
    RAISE NOTICE '  - vw_table_statistics (comprehensive table stats)';
    RAISE NOTICE '  - vw_slow_queries (performance bottlenecks)';
    RAISE NOTICE '';
    RAISE NOTICE 'Maintenance Functions:';
    RAISE NOTICE '  - create_monthly_partition(schema, table, date)';
    RAISE NOTICE '  - create_future_partitions(schema, table, months)';
    RAISE NOTICE '  - reindex_table(schema, table)';
    RAISE NOTICE '  - analyze_schema(schema)';
    RAISE NOTICE '  - vacuum_bloated_tables(threshold)';
    RAISE NOTICE '';
    RAISE NOTICE 'See script comments for recommended maintenance schedule.';
    RAISE NOTICE '============================================================================';
END $$;
