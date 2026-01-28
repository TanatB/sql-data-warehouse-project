from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

from weather_etl.loaders.bronze_loader import SQLExecutor

import psycopg2
import os

from datetime import datetime, timedelta
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

# Redirect Airflow's Docker path to SQL directory
PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR = PROJECT_ROOT / "sql"

default_args = {
    'owner': 'tanat_metmaolee',
    'email': ['bright.tanat@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

@dag(
    dag_id = 'silver_backfill_historical',
    default_args = default_args,
    description = 'Manually backfill historical bronze data to silver.',
    schedule = None, # only for manual trigger
    start_date = datetime(2026, 1, 1),
    catchup = False,
    tags = ['silver', 'backfill', 'one-time', 'etl'],
)
def silver_backfill_pipeline():
    """_summary_
    """
    @task
    def backfill_all_bronze_to_silver(start_date : str = None, end_date : str = None):
        """_summary_

        Args:
            start_date (str, optional): _description_. Defaults to None.
            end_date (str, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )
        try:
            cursor = conn.cursor()
            
            sql_file = SQL_DIR / "02_silver" / "silver_layer.sql"

            with open(sql_file, 'r') as f:
                sql_script = f.read()
            
            if start_date is None and end_date is None:
                where_clause = "WHERE 1=1"
                logger.info("Backfilling ALL bronze records")

            elif start_date is not None and end_date is None:
                where_clause = f"WHERE w.created_at >= '{start_date}'::TIMESTAMPTZ"
                logger.info(f"Backfilling records from {start_date} onwards")

            elif start_date is not None and end_date is not None:
                where_clause = f"WHERE w.created_at >= '{start_date}'::TIMESTAMPTZ AND w.created_at <= '{end_date}'::TIMESTAMPTZ"
                logger.info(f"Backfilling records from {start_date} to {end_date}")
            
            else:
                where_clause = f"WHERE w.created_at <= '{end_date}'::TIMESTAMPTZ"
                logger.info(f"Backfilling records up to {end_date}")

            sql_script = sql_script.replace(
                "WHERE w.created_at > NOW() - INTERVAL '1 hour'",
                where_clause
            )

            logger.info(f"Executing backfill transformation...")
            cursor.execute(sql_script)
            conn.commit()

            # Count results
            cursor.execute("SELECT COUNT(*) FROM silver.weather_observations")
            total_count = cursor.fetchone()[0]

            logger.info(f"✅ Backfill complete! Total silver records: {total_count}")
            
            return {
                "status": "completed", 
                "total_records": total_count,
                "start_date": start_date,
                "end_date": end_date
            }
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error, backfill failed: {e}")
            raise

        finally:
            cursor.close()
            conn.close()

    @task
    def verify_backfill(backfill_result: dict):
        """_summary_

        Args:
            backfill_result (dict): _description_

        Returns:
            _type_: _description_
        """
        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    MIN(observation_timestamp) as earliest_date,
                    MAX(observation_timestamp) as latest_date,
                    COUNT(DISTINCT DATE(observation_timestamp)) as unique_days
                FROM silver.weather_observations
            """)
            
            result = cursor.fetchone()
            
            logger.info(f" Silver Layer Statistics:")
            logger.info(f"   Total records: {result[0]}")
            logger.info(f"   Earliest observation: {result[1]}")
            logger.info(f"   Latest observation: {result[2]}")
            logger.info(f"   Unique days: {result[3]}")
            
            return {
                "total_records": result[0],
                "earliest": str(result[1]),
                "latest": str(result[2]),
                "unique_days": result[3]
            }
        
        finally:
            cursor.close()
            conn.close()
        
        return {0:1}


    # FLOW
    result = backfill_all_bronze_to_silver()
    verify_backfill(result)


dag = silver_backfill_pipeline()