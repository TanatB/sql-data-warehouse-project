from airflow.sdk import dag, task
from airflow.datasets import Dataset

from weather_etl.loaders.bronze_loader import SQLExecutor

import psycopg2

import os, logging
from datetime import datetime, timedelta
from pathlib import Path

# Redirect Airflow's Docker path to SQL directory
PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR = PROJECT_ROOT / "sql"

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'tanat_metmaolee',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes = 30),
}

# Dataset Dependencies for Airflow 3.0+
# URI is just a convention (can be anything) but I use this for self-documenting
bronze_dataset = Dataset("bronze_weather_raw")
silver_dataset = Dataset("silver_weather_observations")
gold_dataset = Dataset("gold_weather_summary")

@dag(
    dag_id = 'gold_layer_pipeline',
    default_args = default_args,    # DAG name
    description = 'Aggregate the silver layer records into one daily ' \
    'gold record for weather summary.',
    schedule = [silver_dataset], # Triggered once a silver dag is completed
    start_date = datetime(2026, 1, 1),
    catchup = False,    # Don't backfill historical runs
    max_active_runs = 1, # run one at a time 
    tags = ['gold', 'data_aggregation', 'etl'],
)
def gold_layer_pipeline():
    """_summary_
    """
    @task
    def create_gold_layer_table():
        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )

        try:
            sql_executor = SQLExecutor(conn)
            sql_executor.execute_file(SQL_DIR / "03_gold" / "gold_layer.sql")
            conn.commit()
            logging.info("Gold layer aggregation completed.")
            return {"status": "completed"}
        except Exception as e:
            logger.error(f"Gold layer aggregation failed: {e}")
            raise
        finally:
            conn.close()

    create_gold_layer_table()

dag = gold_layer_pipeline()