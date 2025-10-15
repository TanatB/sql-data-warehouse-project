-- ============================================================================
-- Medallion Architecture Database Initialization Script
-- ============================================================================
-- Purpose: Initialize data warehouse with Bronze, Silver, and Gold layers
-- Database: PostgreSQL 16
-- Architecture: Medallion (Bronze -> Silver -> Gold)
-- ============================================================================

-- ============================================================================
-- SECTION 1: DROP EXISTING OBJECTS (Use with caution!)
-- ============================================================================
-- Uncomment the following lines if you need to recreate the database from scratch
-- DROP DATABASE IF EXISTS warehouse_db;
-- DROP ROLE IF EXISTS dw_admin;
-- DROP ROLE IF EXISTS dw_developer;
-- DROP ROLE IF EXISTS dw_analyst;
-- DROP ROLE IF EXISTS dw_readonly;
-- DROP ROLE IF EXISTS airflow_service;

-- ============================================================================
-- SECTION 2: CREATE ROLES (Role-Based Access Control)
-- ============================================================================

-- Admin role: Full privileges across all schemas
CREATE ROLE dw_admin WITH
    LOGIN
    CREATEDB
    CREATEROLE
    PASSWORD 'change_me_admin_password'
    VALID UNTIL 'infinity';

COMMENT ON ROLE dw_admin IS 'Data Warehouse Administrator - Full access to all objects';

-- Developer role: Read/Write access to Bronze and Silver, Read access to Gold
CREATE ROLE dw_developer WITH
    LOGIN
    PASSWORD 'change_me_developer_password'
    VALID UNTIL 'infinity';

COMMENT ON ROLE dw_developer IS 'Data Engineer/Developer - Read/Write to Bronze/Silver, Read-only to Gold';

-- Analyst role: Read access to Silver and Gold, no access to Bronze
CREATE ROLE dw_analyst WITH
    LOGIN
    PASSWORD 'change_me_analyst_password'
    VALID UNTIL 'infinity';

COMMENT ON ROLE dw_analyst IS 'Business Analyst - Read-only access to Silver and Gold layers';

-- Read-only role: Read access to Gold layer only
CREATE ROLE dw_readonly WITH
    LOGIN
    PASSWORD 'change_me_readonly_password'
    VALID UNTIL 'infinity';

COMMENT ON ROLE dw_readonly IS 'Read-Only User - Read access to Gold layer only';

-- Service account for Airflow/ETL processes
CREATE ROLE airflow_service WITH
    LOGIN
    PASSWORD 'change_me_airflow_service_password'
    VALID UNTIL 'infinity';

COMMENT ON ROLE airflow_service IS 'Service account for Airflow ETL processes - Full access to all layers';

-- ============================================================================
-- SECTION 3: CREATE INDIVIDUAL USER ACCOUNTS (Examples)
-- ============================================================================
-- These are example user accounts. Adjust as needed for your team.

-- Admin users
CREATE USER admin_user1 WITH
    LOGIN
    PASSWORD 'change_me_user1_password'
    IN ROLE dw_admin;

-- Developer users
CREATE USER developer_user1 WITH
    LOGIN
    PASSWORD 'change_me_dev1_password'
    IN ROLE dw_developer;

CREATE USER developer_user2 WITH
    LOGIN
    PASSWORD 'change_me_dev2_password'
    IN ROLE dw_developer;

-- Analyst users
CREATE USER analyst_user1 WITH
    LOGIN
    PASSWORD 'change_me_analyst1_password'
    IN ROLE dw_analyst;

-- Read-only users
CREATE USER readonly_user1 WITH
    LOGIN
    PASSWORD 'change_me_readonly1_password'
    IN ROLE dw_readonly;

-- ============================================================================
-- SECTION 4: CREATE DATABASE AND CONNECT
-- ============================================================================

-- Create the warehouse database
CREATE DATABASE warehouse_db
    WITH
    OWNER = dw_admin
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    TEMPLATE = template0;

COMMENT ON DATABASE warehouse_db IS 'Data Warehouse with Medallion Architecture (Bronze/Silver/Gold)';

-- Grant connection privileges
GRANT CONNECT ON DATABASE warehouse_db TO dw_admin;
GRANT CONNECT ON DATABASE warehouse_db TO dw_developer;
GRANT CONNECT ON DATABASE warehouse_db TO dw_analyst;
GRANT CONNECT ON DATABASE warehouse_db TO dw_readonly;
GRANT CONNECT ON DATABASE warehouse_db TO airflow_service;

-- Connect to the warehouse database
\c warehouse_db

-- ============================================================================
-- SECTION 5: CREATE SCHEMAS (Medallion Architecture)
-- ============================================================================

-- Bronze schema: Raw data layer (landing zone)
CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION dw_admin;

COMMENT ON SCHEMA bronze IS 'Bronze Layer - Raw data as ingested from source systems';

-- Silver schema: Cleaned and conformed data layer
CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION dw_admin;

COMMENT ON SCHEMA silver IS 'Silver Layer - Cleaned, validated, and conformed data';

-- Gold schema: Business-level aggregates and dimensional models
CREATE SCHEMA IF NOT EXISTS gold
    AUTHORIZATION dw_admin;

COMMENT ON SCHEMA gold IS 'Gold Layer - Business-ready dimensional models and aggregates';

-- Staging schema: Temporary workspace for ETL processes
CREATE SCHEMA IF NOT EXISTS staging
    AUTHORIZATION dw_admin;

COMMENT ON SCHEMA staging IS 'Staging Layer - Temporary workspace for ETL transformations';

-- Audit schema: Data lineage and audit logs
CREATE SCHEMA IF NOT EXISTS audit
    AUTHORIZATION dw_admin;

COMMENT ON SCHEMA audit IS 'Audit Layer - Data lineage, ETL logs, and audit trails';

-- ============================================================================
-- SECTION 6: CREATE EXTENSIONS
-- ============================================================================

-- UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
    SCHEMA public
    VERSION "1.1";

-- Cryptographic functions
CREATE EXTENSION IF NOT EXISTS pgcrypto
    SCHEMA public;

-- Advanced text search
CREATE EXTENSION IF NOT EXISTS pg_trgm
    SCHEMA public;

-- ============================================================================
-- SECTION 7: GRANT SCHEMA PERMISSIONS
-- ============================================================================

-- Bronze Schema Permissions
GRANT USAGE ON SCHEMA bronze TO dw_developer, airflow_service;
GRANT CREATE ON SCHEMA bronze TO dw_developer, airflow_service;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO dw_admin;

-- Silver Schema Permissions
GRANT USAGE ON SCHEMA silver TO dw_developer, dw_analyst, airflow_service;
GRANT CREATE ON SCHEMA silver TO dw_developer, airflow_service;
GRANT ALL PRIVILEGES ON SCHEMA silver TO dw_admin;

-- Gold Schema Permissions
GRANT USAGE ON SCHEMA gold TO dw_developer, dw_analyst, dw_readonly, airflow_service;
GRANT CREATE ON SCHEMA gold TO dw_admin, airflow_service;
GRANT ALL PRIVILEGES ON SCHEMA gold TO dw_admin;

-- Staging Schema Permissions
GRANT USAGE ON SCHEMA staging TO dw_developer, airflow_service;
GRANT CREATE ON SCHEMA staging TO dw_developer, airflow_service;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dw_admin;

-- Audit Schema Permissions
GRANT USAGE ON SCHEMA audit TO dw_admin, airflow_service;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO dw_developer, dw_analyst;
GRANT ALL PRIVILEGES ON SCHEMA audit TO dw_admin;

-- ============================================================================
-- SECTION 8: SET DEFAULT PRIVILEGES FOR FUTURE OBJECTS
-- ============================================================================

-- Default privileges for Bronze schema
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dw_developer, airflow_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT SELECT ON TABLES TO dw_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT USAGE, SELECT ON SEQUENCES TO dw_developer, airflow_service;

-- Default privileges for Silver schema
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dw_developer, airflow_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT SELECT ON TABLES TO dw_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT USAGE, SELECT ON SEQUENCES TO dw_developer, airflow_service;

-- Default privileges for Gold schema
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT SELECT ON TABLES TO dw_developer, dw_analyst, dw_readonly, airflow_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT INSERT, UPDATE, DELETE ON TABLES TO airflow_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT USAGE, SELECT ON SEQUENCES TO dw_developer, dw_analyst, airflow_service;

-- Default privileges for Staging schema
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT ALL ON TABLES TO dw_developer, airflow_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT ALL ON SEQUENCES TO dw_developer, airflow_service;

-- Default privileges for Audit schema
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
    GRANT SELECT ON TABLES TO dw_developer, dw_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
    GRANT INSERT, UPDATE ON TABLES TO airflow_service;

-- ============================================================================
-- SECTION 9: CREATE AUDIT TABLES
-- ============================================================================

-- ETL execution log table
CREATE TABLE IF NOT EXISTS audit.etl_execution_log (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(255),
    execution_start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    execution_end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_deleted INTEGER,
    error_message TEXT,
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    metadata JSONB
);

CREATE INDEX idx_etl_log_job_name ON audit.etl_execution_log(job_name);
CREATE INDEX idx_etl_log_status ON audit.etl_execution_log(status);
CREATE INDEX idx_etl_log_start_time ON audit.etl_execution_log(execution_start_time DESC);

COMMENT ON TABLE audit.etl_execution_log IS 'Logs all ETL job executions with metrics and status';

-- Data quality log table
CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    quality_check_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID REFERENCES audit.etl_execution_log(execution_id),
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_type VARCHAR(100) NOT NULL, -- 'NULL_CHECK', 'DUPLICATE_CHECK', 'RANGE_CHECK', etc.
    check_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    check_passed BOOLEAN NOT NULL,
    failed_records_count INTEGER,
    check_details JSONB,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX idx_dq_log_table ON audit.data_quality_log(schema_name, table_name);
CREATE INDEX idx_dq_log_timestamp ON audit.data_quality_log(check_timestamp DESC);
CREATE INDEX idx_dq_log_passed ON audit.data_quality_log(check_passed);

COMMENT ON TABLE audit.data_quality_log IS 'Tracks data quality checks and validation results';

-- Table lineage tracking
CREATE TABLE IF NOT EXISTS audit.table_lineage (
    lineage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_schema VARCHAR(100) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    target_schema VARCHAR(100) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    transformation_type VARCHAR(100), -- 'EXTRACT', 'TRANSFORM', 'LOAD', 'AGGREGATE'
    lineage_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_lineage_source ON audit.table_lineage(source_schema, source_table);
CREATE INDEX idx_lineage_target ON audit.table_lineage(target_schema, target_table);

COMMENT ON TABLE audit.table_lineage IS 'Tracks data lineage across Bronze, Silver, and Gold layers';

-- ============================================================================
-- SECTION 10: CREATE UTILITY FUNCTIONS
-- ============================================================================

-- Function to log ETL start
CREATE OR REPLACE FUNCTION audit.log_etl_start(
    p_job_name VARCHAR,
    p_schema_name VARCHAR,
    p_table_name VARCHAR DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_execution_id UUID;
BEGIN
    INSERT INTO audit.etl_execution_log (
        job_name,
        schema_name,
        table_name,
        execution_start_time,
        status,
        metadata
    ) VALUES (
        p_job_name,
        p_schema_name,
        p_table_name,
        CURRENT_TIMESTAMP,
        'RUNNING',
        p_metadata
    ) RETURNING execution_id INTO v_execution_id;

    RETURN v_execution_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.log_etl_start IS 'Logs the start of an ETL job execution';

-- Function to log ETL completion
CREATE OR REPLACE FUNCTION audit.log_etl_complete(
    p_execution_id UUID,
    p_status VARCHAR,
    p_rows_inserted INTEGER DEFAULT 0,
    p_rows_updated INTEGER DEFAULT 0,
    p_rows_deleted INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE audit.etl_execution_log
    SET execution_end_time = CURRENT_TIMESTAMP,
        status = p_status,
        rows_inserted = p_rows_inserted,
        rows_updated = p_rows_updated,
        rows_deleted = p_rows_deleted,
        error_message = p_error_message
    WHERE execution_id = p_execution_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.log_etl_complete IS 'Logs the completion of an ETL job execution';

-- ============================================================================
-- SECTION 11: CREATE COMMON DATA TYPES
-- ============================================================================

-- Create enum for data quality status
DO $$ BEGIN
    CREATE TYPE audit.data_quality_status AS ENUM ('PASSED', 'FAILED', 'WARNING');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Create enum for ETL status
DO $$ BEGIN
    CREATE TYPE audit.etl_status AS ENUM ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ============================================================================
-- SECTION 12: CONFIGURE DATABASE SETTINGS
-- ============================================================================

-- Set search path to include all schemas
ALTER DATABASE warehouse_db SET search_path TO public, bronze, silver, gold, staging, audit;

-- Configure statement timeout (10 minutes)
ALTER DATABASE warehouse_db SET statement_timeout = '600s';

-- Configure work memory for better query performance
ALTER DATABASE warehouse_db SET work_mem = '64MB';

-- Configure maintenance work memory
ALTER DATABASE warehouse_db SET maintenance_work_mem = '256MB';

-- Enable parallel query execution
ALTER DATABASE warehouse_db SET max_parallel_workers_per_gather = 4;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Database initialization completed successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Database: warehouse_db';
    RAISE NOTICE 'Schemas created: bronze, silver, gold, staging, audit';
    RAISE NOTICE 'Roles created: dw_admin, dw_developer, dw_analyst, dw_readonly, airflow_service';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Update all passwords in this script before running in production';
    RAISE NOTICE '2. Run ddl_bronze.sql to create Bronze layer tables';
    RAISE NOTICE '3. Run ddl_silver.sql to create Silver layer tables';
    RAISE NOTICE '4. Run ddl_gold.sql to create Gold layer dimension and fact tables';
    RAISE NOTICE '5. Review and apply role_management.sql for fine-grained permissions';
    RAISE NOTICE '============================================================================';
END $$;
