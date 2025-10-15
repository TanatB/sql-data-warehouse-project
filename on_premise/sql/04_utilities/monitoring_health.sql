-- ============================================================================
-- Database Monitoring and Health Check Script
-- ============================================================================
-- Purpose: Monitor database health, connections, locks, and performance
-- Usage: Run periodically or integrate with monitoring tools
-- ============================================================================

\c warehouse_db

-- ============================================================================
-- SECTION 1: CONNECTION MONITORING
-- ============================================================================

-- View: Active connections by user and database
CREATE OR REPLACE VIEW public.vw_active_connections AS
SELECT
    datname as database_name,
    usename as user_name,
    application_name,
    client_addr as client_ip,
    client_port,
    backend_start,
    state,
    state_change,
    query_start,
    CURRENT_TIMESTAMP - query_start as query_duration,
    wait_event_type,
    wait_event,
    LEFT(query, 100) as query_snippet
FROM pg_stat_activity
WHERE datname = 'warehouse_db'
    AND pid <> pg_backend_pid()  -- Exclude current session
ORDER BY query_start DESC NULLS LAST;

COMMENT ON VIEW public.vw_active_connections IS 'Real-time view of active database connections';

-- View: Connection summary by state
CREATE OR REPLACE VIEW public.vw_connection_summary AS
SELECT
    state,
    COUNT(*) as connection_count,
    MAX(CURRENT_TIMESTAMP - state_change) as max_duration,
    AVG(CURRENT_TIMESTAMP - state_change) as avg_duration
FROM pg_stat_activity
WHERE datname = 'warehouse_db'
    AND pid <> pg_backend_pid()
GROUP BY state
ORDER BY connection_count DESC;

COMMENT ON VIEW public.vw_connection_summary IS 'Summary of connections grouped by state';

-- ============================================================================
-- SECTION 2: LOCK MONITORING
-- ============================================================================

-- View: Current locks
CREATE OR REPLACE VIEW public.vw_current_locks AS
SELECT
    l.locktype,
    l.database,
    l.relation::regclass as table_name,
    l.page,
    l.tuple,
    l.virtualxid,
    l.transactionid,
    l.mode,
    l.granted,
    a.usename as user_name,
    a.application_name,
    a.client_addr as client_ip,
    a.backend_start,
    a.query_start,
    CURRENT_TIMESTAMP - a.query_start as lock_duration,
    a.state,
    LEFT(a.query, 100) as query_snippet
FROM pg_catalog.pg_locks l
LEFT JOIN pg_catalog.pg_stat_activity a ON l.pid = a.pid
WHERE l.database = (SELECT oid FROM pg_database WHERE datname = 'warehouse_db')
    OR l.database IS NULL
ORDER BY l.granted, a.query_start;

COMMENT ON VIEW public.vw_current_locks IS 'Current locks in the database with query details';

-- View: Blocking queries
CREATE OR REPLACE VIEW public.vw_blocking_queries AS
SELECT
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_query,
    blocking_activity.query AS blocking_query,
    blocked_activity.application_name AS blocked_app,
    blocking_activity.application_name AS blocking_app
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

COMMENT ON VIEW public.vw_blocking_queries IS 'Identifies queries that are blocking other queries';

-- ============================================================================
-- SECTION 3: DATABASE SIZE AND GROWTH MONITORING
-- ============================================================================

-- View: Database size
CREATE OR REPLACE VIEW public.vw_database_size AS
SELECT
    datname as database_name,
    pg_size_pretty(pg_database_size(datname)) as database_size,
    pg_database_size(datname) as size_bytes
FROM pg_database
WHERE datname = 'warehouse_db';

COMMENT ON VIEW public.vw_database_size IS 'Total database size';

-- View: Schema sizes
CREATE OR REPLACE VIEW public.vw_schema_sizes AS
SELECT
    schemaname as schema_name,
    COUNT(*) as table_count,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_size,
    pg_size_pretty(SUM(pg_relation_size(schemaname||'.'||tablename))) as table_size,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename) -
                       pg_relation_size(schemaname||'.'||tablename))) as index_size,
    SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total_bytes
FROM pg_catalog.pg_tables
WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit', 'staging')
GROUP BY schemaname
ORDER BY total_bytes DESC;

COMMENT ON VIEW public.vw_schema_sizes IS 'Size breakdown by schema';

-- View: Top 20 largest tables
CREATE OR REPLACE VIEW public.vw_largest_tables AS
SELECT
    schemaname as schema_name,
    tablename as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) -
                   pg_relation_size(schemaname||'.'||tablename)) as index_size,
    pg_total_relation_size(schemaname||'.'||tablename) as total_bytes,
    (SELECT COUNT(*) FROM pg_catalog.pg_index WHERE indrelid = (schemaname||'.'||tablename)::regclass) as index_count
FROM pg_catalog.pg_tables
WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit', 'staging')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;

COMMENT ON VIEW public.vw_largest_tables IS 'Top 20 largest tables by total size';

-- ============================================================================
-- SECTION 4: CACHE HIT RATIO MONITORING
-- ============================================================================

-- View: Cache hit ratios
CREATE OR REPLACE VIEW public.vw_cache_hit_ratios AS
SELECT
    'Index Hit Rate' as metric,
    ROUND(100.0 * SUM(idx_blks_hit) / NULLIF(SUM(idx_blks_hit + idx_blks_read), 0), 2) as percentage
FROM pg_statio_user_indexes
UNION ALL
SELECT
    'Table Hit Rate' as metric,
    ROUND(100.0 * SUM(heap_blks_hit) / NULLIF(SUM(heap_blks_hit + heap_blks_read), 0), 2) as percentage
FROM pg_statio_user_tables
UNION ALL
SELECT
    'Overall Hit Rate' as metric,
    ROUND(100.0 * SUM(blks_hit) / NULLIF(SUM(blks_hit + blks_read), 0), 2) as percentage
FROM pg_stat_database
WHERE datname = 'warehouse_db';

COMMENT ON VIEW public.vw_cache_hit_ratios IS 'Cache hit ratios - should be > 95% for good performance';

-- ============================================================================
-- SECTION 5: TRANSACTION AND QUERY STATISTICS
-- ============================================================================

-- View: Transaction statistics
CREATE OR REPLACE VIEW public.vw_transaction_stats AS
SELECT
    datname as database_name,
    xact_commit as committed_transactions,
    xact_rollback as rolled_back_transactions,
    CASE
        WHEN (xact_commit + xact_rollback) > 0 THEN
            ROUND(100.0 * xact_rollback / (xact_commit + xact_rollback), 2)
        ELSE 0
    END as rollback_percentage,
    blks_read as blocks_read_from_disk,
    blks_hit as blocks_read_from_cache,
    tup_returned as rows_returned,
    tup_fetched as rows_fetched,
    tup_inserted as rows_inserted,
    tup_updated as rows_updated,
    tup_deleted as rows_deleted,
    conflicts,
    temp_files,
    pg_size_pretty(temp_bytes) as temp_file_size
FROM pg_stat_database
WHERE datname = 'warehouse_db';

COMMENT ON VIEW public.vw_transaction_stats IS 'Transaction and query statistics for the database';

-- ============================================================================
-- SECTION 6: REPLICATION AND BACKUP MONITORING
-- ============================================================================

-- View: Replication status (if replication is configured)
CREATE OR REPLACE VIEW public.vw_replication_status AS
SELECT
    client_addr as replica_address,
    state,
    sync_state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    pg_wal_lsn_diff(sent_lsn, replay_lsn) as replication_lag_bytes,
    pg_size_pretty(pg_wal_lsn_diff(sent_lsn, replay_lsn)::numeric) as replication_lag,
    application_name,
    backend_start,
    reply_time
FROM pg_stat_replication;

COMMENT ON VIEW public.vw_replication_status IS 'Replication status and lag (if replication is configured)';

-- ============================================================================
-- SECTION 7: HEALTH CHECK FUNCTIONS
-- ============================================================================

-- Function: Comprehensive health check
CREATE OR REPLACE FUNCTION public.health_check()
RETURNS TABLE(
    check_name VARCHAR,
    status VARCHAR,
    value TEXT,
    threshold TEXT,
    message TEXT
) AS $$
DECLARE
    v_cache_hit_ratio DECIMAL;
    v_connection_count INTEGER;
    v_max_connections INTEGER;
    v_dead_tuple_avg DECIMAL;
    v_long_running_queries INTEGER;
    v_blocking_queries INTEGER;
BEGIN
    -- Check 1: Cache hit ratio
    SELECT COALESCE(
        (SELECT percentage FROM public.vw_cache_hit_ratios WHERE metric = 'Overall Hit Rate'),
        0
    ) INTO v_cache_hit_ratio;

    check_name := 'Cache Hit Ratio';
    value := v_cache_hit_ratio || '%';
    threshold := '> 95%';
    IF v_cache_hit_ratio >= 95 THEN
        status := 'PASS';
        message := 'Cache hit ratio is healthy';
    ELSIF v_cache_hit_ratio >= 90 THEN
        status := 'WARN';
        message := 'Cache hit ratio is below optimal';
    ELSE
        status := 'FAIL';
        message := 'Cache hit ratio is critically low - consider increasing shared_buffers';
    END IF;
    RETURN NEXT;

    -- Check 2: Connection usage
    SELECT COUNT(*) INTO v_connection_count
    FROM pg_stat_activity
    WHERE datname = 'warehouse_db';

    SELECT setting::INTEGER INTO v_max_connections
    FROM pg_settings
    WHERE name = 'max_connections';

    check_name := 'Connection Usage';
    value := v_connection_count || ' / ' || v_max_connections;
    threshold := '< 80% of max';
    IF v_connection_count < (v_max_connections * 0.8) THEN
        status := 'PASS';
        message := 'Connection usage is healthy';
    ELSIF v_connection_count < (v_max_connections * 0.9) THEN
        status := 'WARN';
        message := 'Connection usage is high';
    ELSE
        status := 'FAIL';
        message := 'Connection usage is critical - risk of hitting max connections';
    END IF;
    RETURN NEXT;

    -- Check 3: Dead tuples
    SELECT COALESCE(AVG(
        CASE
            WHEN n_live_tup > 0 THEN
                100.0 * n_dead_tup / (n_live_tup + n_dead_tup)
            ELSE 0
        END
    ), 0) INTO v_dead_tuple_avg
    FROM pg_stat_user_tables
    WHERE schemaname IN ('bronze', 'silver', 'gold');

    check_name := 'Dead Tuple Percentage';
    value := ROUND(v_dead_tuple_avg, 2) || '%';
    threshold := '< 10%';
    IF v_dead_tuple_avg < 10 THEN
        status := 'PASS';
        message := 'Dead tuple percentage is healthy';
    ELSIF v_dead_tuple_avg < 20 THEN
        status := 'WARN';
        message := 'Dead tuple percentage is elevated - consider VACUUM';
    ELSE
        status := 'FAIL';
        message := 'Dead tuple percentage is high - run VACUUM immediately';
    END IF;
    RETURN NEXT;

    -- Check 4: Long running queries
    SELECT COUNT(*) INTO v_long_running_queries
    FROM pg_stat_activity
    WHERE datname = 'warehouse_db'
        AND state = 'active'
        AND (CURRENT_TIMESTAMP - query_start) > INTERVAL '5 minutes';

    check_name := 'Long Running Queries';
    value := v_long_running_queries || ' queries > 5 min';
    threshold := '0';
    IF v_long_running_queries = 0 THEN
        status := 'PASS';
        message := 'No long running queries detected';
    ELSIF v_long_running_queries < 3 THEN
        status := 'WARN';
        message := 'Some long running queries detected';
    ELSE
        status := 'FAIL';
        message := 'Multiple long running queries - investigate performance issues';
    END IF;
    RETURN NEXT;

    -- Check 5: Blocking queries
    SELECT COUNT(*) INTO v_blocking_queries
    FROM public.vw_blocking_queries;

    check_name := 'Blocking Queries';
    value := v_blocking_queries || '';
    threshold := '0';
    IF v_blocking_queries = 0 THEN
        status := 'PASS';
        message := 'No blocking queries detected';
    ELSIF v_blocking_queries < 3 THEN
        status := 'WARN';
        message := 'Some queries are blocking others';
    ELSE
        status := 'FAIL';
        message := 'Multiple blocking queries - investigate locks';
    END IF;
    RETURN NEXT;

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.health_check IS 'Performs comprehensive health check and returns status';

-- ============================================================================
-- SECTION 8: ALERT FUNCTIONS
-- ============================================================================

-- Function: Kill idle connections
CREATE OR REPLACE FUNCTION public.kill_idle_connections(
    p_idle_threshold INTERVAL DEFAULT '30 minutes'
)
RETURNS TABLE(
    pid INTEGER,
    user_name VARCHAR,
    database_name VARCHAR,
    idle_duration INTERVAL,
    status TEXT
) AS $$
DECLARE
    v_connection RECORD;
BEGIN
    FOR v_connection IN
        SELECT
            a.pid,
            a.usename,
            a.datname,
            CURRENT_TIMESTAMP - a.state_change as idle_time
        FROM pg_stat_activity a
        WHERE a.datname = 'warehouse_db'
            AND a.state = 'idle'
            AND (CURRENT_TIMESTAMP - a.state_change) > p_idle_threshold
            AND a.pid <> pg_backend_pid()
    LOOP
        BEGIN
            PERFORM pg_terminate_backend(v_connection.pid);

            pid := v_connection.pid;
            user_name := v_connection.usename;
            database_name := v_connection.datname;
            idle_duration := v_connection.idle_time;
            status := 'KILLED';
            RETURN NEXT;
        EXCEPTION
            WHEN OTHERS THEN
                pid := v_connection.pid;
                user_name := v_connection.usename;
                database_name := v_connection.datname;
                idle_duration := v_connection.idle_time;
                status := 'ERROR: ' || SQLERRM;
                RETURN NEXT;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.kill_idle_connections IS 'Terminates idle connections exceeding threshold';

-- ============================================================================
-- SECTION 9: GRANT PERMISSIONS
-- ============================================================================

-- Grant view access
GRANT SELECT ON public.vw_active_connections TO dw_developer, dw_analyst;
GRANT SELECT ON public.vw_connection_summary TO dw_developer, dw_analyst;
GRANT SELECT ON public.vw_current_locks TO dw_developer;
GRANT SELECT ON public.vw_blocking_queries TO dw_developer;
GRANT SELECT ON public.vw_database_size TO dw_developer, dw_analyst, dw_readonly;
GRANT SELECT ON public.vw_schema_sizes TO dw_developer, dw_analyst, dw_readonly;
GRANT SELECT ON public.vw_largest_tables TO dw_developer, dw_analyst;
GRANT SELECT ON public.vw_cache_hit_ratios TO dw_developer;
GRANT SELECT ON public.vw_transaction_stats TO dw_developer;
GRANT SELECT ON public.vw_replication_status TO dw_admin;

-- Grant function execution
GRANT EXECUTE ON FUNCTION public.health_check TO dw_developer, dw_admin;
GRANT EXECUTE ON FUNCTION public.kill_idle_connections TO dw_admin;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Monitoring and Health Check Script completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Monitoring Views:';
    RAISE NOTICE '  - vw_active_connections';
    RAISE NOTICE '  - vw_connection_summary';
    RAISE NOTICE '  - vw_current_locks';
    RAISE NOTICE '  - vw_blocking_queries';
    RAISE NOTICE '  - vw_database_size';
    RAISE NOTICE '  - vw_schema_sizes';
    RAISE NOTICE '  - vw_largest_tables';
    RAISE NOTICE '  - vw_cache_hit_ratios';
    RAISE NOTICE '  - vw_transaction_stats';
    RAISE NOTICE '  - vw_replication_status';
    RAISE NOTICE '';
    RAISE NOTICE 'Health Check Functions:';
    RAISE NOTICE '  - SELECT * FROM health_check();';
    RAISE NOTICE '  - SELECT * FROM kill_idle_connections(''30 minutes'');';
    RAISE NOTICE '';
    RAISE NOTICE 'Run health_check() regularly to monitor database health!';
    RAISE NOTICE '============================================================================';
END $$;
