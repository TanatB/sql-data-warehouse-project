from airflow.sdk import dag, task, Param
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
    'retry_delay': timedelta(minutes = 2),
}

@dag(
    dag_id = 'silver_backfill_historical',
    default_args = default_args,
    description = 'Manually backfill historical bronze data to silver.',
    schedule = None, # only for manual trigger
    start_date = datetime(2026, 1, 1),
    catchup = False,
    tags = ['silver', 'backfill', 'one-time', 'etl'],
    params = {
        "start_date": Param(
            default=None,
            type=["null", "string"],
            description="Start date for backfill (YYYY-MM-DDTHH:MM:SS), default None"
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            description="End date date for backfill (YYYY-MM-DDTHH:MM:SS), default None"
        ),
    },
)
def silver_backfill_pipeline():
    """
    Pipeline used to backfill-transform historical data from Bronze -> Silver layer.
    """
    @task
    def backfill_bronze_to_silver(**context):
        """
        Run a backfill process with psycopg2 to make sure the insertion is idempotent.

        Returns:
            dict: status (completed), total_records, start_date, end_date
        """
        # Params from DAG run config
        params = context["params"]
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        _log_backfill_mode(start_date=start_date, end_date=end_date)

        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )
        try:
            sql_executor = SQLExecutor(conn)
            
            rows_affected = sql_executor.execute_backfill(
                sql_file_path = SQL_DIR / "02_silver" / "silver_layer.sql",
                start_date = start_date,
                end_date = end_date
            )
            
            total_count = sql_executor.execute_query(
                "SELECT COUNT(*) FROM silver.weather_observations"
            )[0][0]

            logger.info(f"Backfill Complete!")
            logger.info(f"Rows inserted: {rows_affected}")
            logger.info(f" Total silver records: {total_count}")
            
            return {
                "status": "completed", 
                "rows_inserted": rows_affected,
                "total_records": total_count,
                "start_date": start_date,
                "end_date": end_date
            }
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error, backfill failed: {e}")
            raise

        finally:
            conn.close()

    @task
    def verify_backfill(backfill_result: dict):
        """
        Verify the backfill completed successfully.

        Args:
            backfill_result (dict): {status, rows_inserted, total_records, start_date, end_date}

        Raises:
            ValueError: if the backfill_result status is not completed

        Returns:
            dict: status, by_location (dict)
        """
        if backfill_result["status"] != "completed":
            raise ValueError("Backfill did not complete successfully")
    
        conn = psycopg2.connect(
            host="postgres-warehouse",
            port=5432,
            dbname=os.getenv("POSTGRES_WAREHOUSE_DB"),
            user=os.getenv("POSTGRES_WAREHOUSE_USER"),
            password=os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )
        
        try:
            sql_executor = SQLExecutor(conn)
            
            location_counts = sql_executor.execute_query("""
                SELECT location_name, COUNT(*)
                FROM silver.weather_observations
                GROUP BY location_name
                ORDER BY location_name
            """)
            
            logger.info(f"✅ Verification passed!")
            logger.info(f"   Records by location")
            for loc, count in location_counts:
                logger.info(f"   {loc}: {count} records")
            
            return {
                "status": "verified",
                "by_location": dict(location_counts)
            }
        
        finally:
            conn.close()


    # FLOW
    result = backfill_bronze_to_silver()
    verify_backfill(result)


def _log_backfill_mode(start_date: str = None, end_date: str = None):
    """Log what type of backfill we're doing."""
    if start_date is None and end_date is None:
        logger.info("📅 Backfill mode: ALL unprocessed bronze records")
    elif start_date and end_date is None:
        logger.info(f"📅 Backfill mode: From {start_date} onwards")
    elif start_date is None and end_date:
        logger.info(f"📅 Backfill mode: Up to {end_date}")
    else:
        logger.info(f"📅 Backfill mode: From {start_date} to {end_date}")


dag = silver_backfill_pipeline()