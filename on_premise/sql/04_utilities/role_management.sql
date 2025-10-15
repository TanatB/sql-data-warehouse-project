-- ============================================================================
-- Role-Based Access Control (RBAC) Management Script
-- ============================================================================
-- Purpose: Manage user roles, permissions, and access control
-- Usage: Run as database administrator
-- ============================================================================

\c warehouse_db

-- ============================================================================
-- SECTION 1: CREATE ADDITIONAL ROLES
-- ============================================================================

-- ETL Developer role: Full access to Bronze and Silver, read to Gold
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'etl_developer') THEN
        CREATE ROLE etl_developer WITH
            LOGIN
            PASSWORD 'change_me_etl_dev_password'
            VALID UNTIL 'infinity';
        COMMENT ON ROLE etl_developer IS 'ETL Developer - Full access to Bronze/Silver for ETL development';
    END IF;
END $$;

-- BI Analyst role: Read-only access to Gold layer
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bi_analyst') THEN
        CREATE ROLE bi_analyst WITH
            LOGIN
            PASSWORD 'change_me_bi_analyst_password'
            VALID UNTIL 'infinity';
        COMMENT ON ROLE bi_analyst IS 'BI Analyst - Read-only access to Gold layer';
    END IF;
END $$;

-- Data Scientist role: Read access to Silver and Gold
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'data_scientist') THEN
        CREATE ROLE data_scientist WITH
            LOGIN
            PASSWORD 'change_me_data_scientist_password'
            VALID UNTIL 'infinity';
        COMMENT ON ROLE data_scientist IS 'Data Scientist - Read access to Silver and Gold layers';
    END IF;
END $$;

-- ============================================================================
-- SECTION 2: GRANT DATABASE-LEVEL PERMISSIONS
-- ============================================================================

-- Grant connection to database
GRANT CONNECT ON DATABASE warehouse_db TO etl_developer, bi_analyst, data_scientist;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA bronze TO etl_developer;
GRANT USAGE ON SCHEMA silver TO etl_developer, data_scientist;
GRANT USAGE ON SCHEMA gold TO etl_developer, bi_analyst, data_scientist;
GRANT USAGE ON SCHEMA audit TO etl_developer, data_scientist;

-- ============================================================================
-- SECTION 3: GRANT TABLE-LEVEL PERMISSIONS
-- ============================================================================

-- ETL Developer permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO etl_developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO etl_developer;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO etl_developer;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO etl_developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO etl_developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO etl_developer;

-- BI Analyst permissions
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO bi_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA gold TO bi_analyst;

-- Data Scientist permissions
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO data_scientist;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO data_scientist;
GRANT SELECT ON ALL VIEWS IN SCHEMA silver TO data_scientist;
GRANT SELECT ON ALL VIEWS IN SCHEMA gold TO data_scientist;

-- ============================================================================
-- SECTION 4: SET DEFAULT PRIVILEGES FOR FUTURE OBJECTS
-- ============================================================================

-- Default privileges for ETL Developer
ALTER DEFAULT PRIVILEGES FOR ROLE dw_admin IN SCHEMA bronze
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_developer;

ALTER DEFAULT PRIVILEGES FOR ROLE dw_admin IN SCHEMA silver
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_developer;

ALTER DEFAULT PRIVILEGES FOR ROLE dw_admin IN SCHEMA gold
    GRANT SELECT ON TABLES TO etl_developer;

-- Default privileges for BI Analyst
ALTER DEFAULT PRIVILEGES FOR ROLE dw_admin IN SCHEMA gold
    GRANT SELECT ON TABLES TO bi_analyst;

-- Default privileges for Data Scientist
ALTER DEFAULT PRIVILEGES FOR ROLE dw_admin IN SCHEMA silver
    GRANT SELECT ON TABLES TO data_scientist;

ALTER DEFAULT PRIVILEGES FOR ROLE dw_admin IN SCHEMA gold
    GRANT SELECT ON TABLES TO data_scientist;

-- ============================================================================
-- SECTION 5: ROW LEVEL SECURITY (RLS) POLICIES
-- ============================================================================

-- Enable RLS on sensitive tables (example for audit tables)
ALTER TABLE audit.etl_execution_log ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only see their own executions
CREATE POLICY etl_log_user_policy ON audit.etl_execution_log
    FOR SELECT
    TO etl_developer
    USING (created_by = CURRENT_USER);

-- Policy: Admins can see all executions
CREATE POLICY etl_log_admin_policy ON audit.etl_execution_log
    FOR ALL
    TO dw_admin
    USING (true);

-- Policy: Service accounts can see and modify all
CREATE POLICY etl_log_service_policy ON audit.etl_execution_log
    FOR ALL
    TO airflow_service
    USING (true);

-- ============================================================================
-- SECTION 6: FUNCTION EXECUTION PERMISSIONS
-- ============================================================================

-- Grant execute on utility functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bronze TO etl_developer, airflow_service;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA silver TO etl_developer, airflow_service;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gold TO etl_developer, airflow_service;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA audit TO etl_developer, airflow_service;

-- ============================================================================
-- SECTION 7: CREATE ROLE MANAGEMENT FUNCTIONS
-- ============================================================================

-- Function to create a new developer user
CREATE OR REPLACE FUNCTION public.create_developer_user(
    p_username VARCHAR,
    p_password VARCHAR,
    p_full_name VARCHAR DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_result TEXT;
BEGIN
    -- Check if user already exists
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = p_username) THEN
        RETURN 'ERROR: User ' || p_username || ' already exists';
    END IF;

    -- Create user
    EXECUTE format('CREATE USER %I WITH LOGIN PASSWORD %L IN ROLE dw_developer',
                   p_username, p_password);

    -- Add comment if full name provided
    IF p_full_name IS NOT NULL THEN
        EXECUTE format('COMMENT ON ROLE %I IS %L', p_username,
                      'Developer: ' || p_full_name);
    END IF;

    v_result := 'SUCCESS: Created developer user ' || p_username;

    -- Log the action
    INSERT INTO audit.etl_execution_log (
        job_name, schema_name, status, metadata
    ) VALUES (
        'create_user',
        'public',
        'SUCCESS',
        jsonb_build_object('username', p_username, 'role', 'dw_developer')
    );

    RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.create_developer_user IS 'Creates a new developer user with standard permissions';

-- Function to create a new analyst user
CREATE OR REPLACE FUNCTION public.create_analyst_user(
    p_username VARCHAR,
    p_password VARCHAR,
    p_full_name VARCHAR DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_result TEXT;
BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = p_username) THEN
        RETURN 'ERROR: User ' || p_username || ' already exists';
    END IF;

    EXECUTE format('CREATE USER %I WITH LOGIN PASSWORD %L IN ROLE dw_analyst',
                   p_username, p_password);

    IF p_full_name IS NOT NULL THEN
        EXECUTE format('COMMENT ON ROLE %I IS %L', p_username,
                      'Analyst: ' || p_full_name);
    END IF;

    v_result := 'SUCCESS: Created analyst user ' || p_username;

    INSERT INTO audit.etl_execution_log (
        job_name, schema_name, status, metadata
    ) VALUES (
        'create_user',
        'public',
        'SUCCESS',
        jsonb_build_object('username', p_username, 'role', 'dw_analyst')
    );

    RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.create_analyst_user IS 'Creates a new analyst user with read-only Gold access';

-- Function to revoke user access
CREATE OR REPLACE FUNCTION public.revoke_user_access(
    p_username VARCHAR,
    p_reason VARCHAR DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_result TEXT;
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = p_username) THEN
        RETURN 'ERROR: User ' || p_username || ' does not exist';
    END IF;

    -- Revoke all privileges
    EXECUTE format('REVOKE ALL PRIVILEGES ON DATABASE warehouse_db FROM %I', p_username);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze FROM %I', p_username);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver FROM %I', p_username);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold FROM %I', p_username);

    -- Disable login
    EXECUTE format('ALTER USER %I WITH NOLOGIN', p_username);

    v_result := 'SUCCESS: Revoked access for user ' || p_username;

    -- Log the action
    INSERT INTO audit.etl_execution_log (
        job_name, schema_name, status, metadata
    ) VALUES (
        'revoke_user_access',
        'public',
        'SUCCESS',
        jsonb_build_object('username', p_username, 'reason', COALESCE(p_reason, 'Not specified'))
    );

    RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.revoke_user_access IS 'Revokes all access for a user and disables login';

-- ============================================================================
-- SECTION 8: CREATE PERMISSION AUDIT VIEWS
-- ============================================================================

-- View: All user permissions summary
CREATE OR REPLACE VIEW public.vw_user_permissions AS
SELECT
    r.rolname as role_name,
    r.rolcanlogin as can_login,
    r.rolsuper as is_superuser,
    r.rolcreatedb as can_create_db,
    r.rolcreaterole as can_create_role,
    r.rolconnlimit as connection_limit,
    r.rolvaliduntil as valid_until,
    pg_catalog.shobj_description(r.oid, 'pg_authid') as description,
    ARRAY(
        SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid
    ) as member_of
FROM pg_catalog.pg_roles r
WHERE r.rolname NOT LIKE 'pg_%'
ORDER BY r.rolname;

COMMENT ON VIEW public.vw_user_permissions IS 'Summary of all database users and their role memberships';

-- View: Table permissions by role
CREATE OR REPLACE VIEW public.vw_table_permissions AS
SELECT
    n.nspname as schema_name,
    c.relname as table_name,
    r.rolname as role_name,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'SELECT') as has_select,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'INSERT') as has_insert,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'UPDATE') as has_update,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'DELETE') as has_delete,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'TRUNCATE') as has_truncate,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'REFERENCES') as has_references,
    HAS_TABLE_PRIVILEGE(r.oid, c.oid, 'TRIGGER') as has_trigger
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN pg_catalog.pg_roles r
WHERE c.relkind IN ('r', 'p')  -- regular tables and partitioned tables
    AND n.nspname IN ('bronze', 'silver', 'gold', 'audit')
    AND r.rolname NOT LIKE 'pg_%'
    AND r.rolcanlogin = true
ORDER BY n.nspname, c.relname, r.rolname;

COMMENT ON VIEW public.vw_table_permissions IS 'Detailed table-level permissions for each role';

-- ============================================================================
-- SECTION 9: GRANT PERMISSIONS ON MANAGEMENT FUNCTIONS
-- ============================================================================

-- Only admins can create/revoke users
GRANT EXECUTE ON FUNCTION public.create_developer_user TO dw_admin;
GRANT EXECUTE ON FUNCTION public.create_analyst_user TO dw_admin;
GRANT EXECUTE ON FUNCTION public.revoke_user_access TO dw_admin;

-- Allow all authenticated users to view permissions
GRANT SELECT ON public.vw_user_permissions TO PUBLIC;
GRANT SELECT ON public.vw_table_permissions TO PUBLIC;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'RBAC Management Script completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Additional roles created:';
    RAISE NOTICE '  - etl_developer (Bronze/Silver read-write, Gold read)';
    RAISE NOTICE '  - bi_analyst (Gold read-only)';
    RAISE NOTICE '  - data_scientist (Silver/Gold read)';
    RAISE NOTICE '';
    RAISE NOTICE 'Management functions:';
    RAISE NOTICE '  - create_developer_user(username, password, full_name)';
    RAISE NOTICE '  - create_analyst_user(username, password, full_name)';
    RAISE NOTICE '  - revoke_user_access(username, reason)';
    RAISE NOTICE '';
    RAISE NOTICE 'Audit views:';
    RAISE NOTICE '  - vw_user_permissions';
    RAISE NOTICE '  - vw_table_permissions';
    RAISE NOTICE '';
    RAISE NOTICE 'Row-level security enabled on: audit.etl_execution_log';
    RAISE NOTICE '============================================================================';
END $$;
