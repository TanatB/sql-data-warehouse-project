-- DATABASE
-- CREATE DATABASE data_warehouse;
-- CREATE DATABASE airflow_db;

-- SCHEMA
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ROLE
CREATE ROLE bronze_writer WITH NOLOGIN;
CREATE ROLE silver_transformer WITH NOLOGIN;
CREATE ROLE gold_transformer WITH NOLOGIN;
CREATE ROLE analyst_reader WITH NOLOGIN;
CREATE ROLE pipeline_admin WITH NOLOGIN;
CREATE ROLE read_only_base WITH NOLOGIN;

-- GRANT SCHEMA-LEVEL ACCESS TO ROLES
GRANT USAGE ON SCHEMA bronze TO bronze_writer;
GRANT USAGE ON SCHEMA bronze TO silver_transformer;

-- USER
CREATE USER data_engineer_tanat WITH PASSWORD 'thisispassword' LOGIN;


-- ASSIGN ROLES
GRANT bronze_writer TO data_engineer_tanat;