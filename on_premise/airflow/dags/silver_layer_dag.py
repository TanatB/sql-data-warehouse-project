from airflow.sdk import dag, task
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from weather_etl.loaders.bronze_loader import SQLExecutor

import psycopg2

import os, logging
from datetime import datetime, timedelta
from pathlib import Path

# Redirect Airflow path to SQL directory
PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR = PROJECT_ROOT / "sql"

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'tanat_metmaolee',
    'depends_on_past': False,
    'email': ['bright.tanat@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes = 30),
}

# Dataset Dependencies for Airflow 3.0+
# URI is just a convention (can be anything) but I use this for self-documenting
bronze_dataset = Dataset("bronze_weather_raw")

@dag(
    dag_id = 'silver_layer_pipeline',
    default_args = default_args,    # DAG name
    description = 'Extract weather forecast and load it to bronze_layer.SQL',
    schedule = [bronze_dataset], # Triggered when bronze DAG completes
    start_date = datetime(2026, 1, 1),
    catchup = False,    # Don't backfill historical runs
    max_active_runs = 1, # run one at a time 
    tags = ['silver', 'data_transformation', 'etl'],
)
def silver_layer_pipeline():
    """_summary_
    """
    @task
    def transform_bronze_to_silver():
        """
        Transforms bronze JSONB data into structured silver tables.

        Returns:
            Dictionary: status: completed or failed
        """
        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )

        try:
            sql_executor = SQLExecutor(conn)
            sql_executor.execute_file(SQL_DIR / "02_silver" / "silver_layer.sql")
            logging.info("Silver transformation completed.")
            return {"status": "completed"}
        except Exception as e:
            logger.error(f"Silver tranformation failed: {e}")
            raise
        finally:
            conn.close()
        

    @task
    def validate_silver_data(query_result: dict):
        """
        Validate silver layer data quality.
            1. Row count
            2. Null values in critical columns
            3. Data type correctness
            4. Value ranges

        Args:
            query_result (dict): status, bronze_count, silver_count
        """
        if query_result["status"] != "completed":
            raise ValueError("Cannot validate - transformation did not complete")

        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )
        
        try:
            sql_executor = SQLExecutor(conn)
            
            # Row count
            bronze_count = sql_executor.execute_query(
                "SELECT COUNT(*) FROM bronze.weather_raw WHERE created_at > NOW() - INTERVAL '1 hour'"
            )[0][0]

            silver_count = sql_executor.execute_query(
                "SELECT COUNT(*) FROM silver.weather_observations WHERE created_at > NOW() - INTERVAL '1 hour'"
            )[0][0]

            logging.info(f"Bronze: records: {bronze_count}, Silver records: {silver_count}")

            if silver_count == 0:
                raise ValueError("❌ No records created in silver layer!")
            
            # Null Check
            null_check = sql_executor.execute_query("""
                    SELECT
                        COUNT(*) FILTER (WHERE observation_time IS NULL),
                        COUNT(*) FILTER (WHERE temperature_2m IS NULL) as null_temps,
                        COUNT(*) FILTER (WHERE weather_code IS NULL) as null_codes
                    FROM silver.weather_observations
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                """)[0]
            
            logger.info(f"Null counts - times: {null_check[0]}, temps: {null_check[1]}")

            if null_check[0] > 0:
                raise ValueError(f"Found {null_check[0]} null observation times!")
            
            # Duplication Check
            duplicate_count = sql_executor.execute_query("""
                SELECT COUNT(*) FROM (
                        SELECT observation_time, location_name, COUNT(*)
                        FROM silver.weather_observations
                        WHERE created_at > NOW() - INTERVAL '1 hour'
                        GROUP BY observation_time, location_name
                        HAVING COUNT(*) > 1
                ) duplicates
            """)[0][0]
        
            if duplicate_count > 0:
                raise ValueError(f" Found {duplicate_count} duplicate records.")
            
            logging.info("All silver layer validation passed")

            return {
                "status": "success", 
                "bronze_count": bronze_count, 
                "silver_count": silver_count,
                "null_counts": null_check,
                "duplicates": duplicate_count
            }
        
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            raise

        finally:
            conn.close()

    transfrom_result = transform_bronze_to_silver()  
    validate_silver_data(transfrom_result)


dag = silver_layer_pipeline()