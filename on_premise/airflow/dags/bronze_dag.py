from airflow.sdk import dag, task
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import psycopg2

import os, logging
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

LOCATIONS = [
    {
        "location_name": "Bangkok",
        "latitude": 13.754,
        "longitude": 100.5014,
        "timezone": "Asia/Bangkok"
    },

    {
        "location_name": "Tokyo",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "timezone": "Asia/Tokyo"
    },

    {
        "location_name": "Paris",
        "latitude": 48.8534,
        "longitude": 2.3488,
        "timezone": "auto"
    },
]

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
    "uv_index",
    "is_day",
    "rain",
    "showers",
    "snowfall"
]


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

# URI is just a convention (can be anything) but I use this for self-documenting
bronze_dataset = Dataset("postgres://data_warehouse/bronze.weather_raw")

@dag(
    dag_id = 'weather_extraction_daily',
    default_args = default_args,    # DAG name
    description = 'Extract weather forecast and load it to bronze_layer.SQL',
    schedule = '0 6 * * *', # daily at 6AM UTC
    start_date = datetime(2026, 1, 1),
    catchup = False,    # Don't backfill historical runs
    max_active_runs = 1, # run one at a time 
    tags = ['bronze', 'weather_forecast', 'etl'],
)
def weather_etl_pipeline():
    """Weather ETL Pipeline using TaskFlow API.

    Extract -> Load Bronze -> Transform to Silver (Later in the new script)
    """

    @task(retries=3, retry_delay=timedelta(minutes = 2))
    def extract_city_weather_data(location_config: dict, **context) -> dict:
        """
        Extract weather data from Open-Meteo API.

        Args:
            location_config (dict): latitude, longtitude, location_name, timezone

        Returns:
            dict: api_response & metadata
        """
        from weather_etl.extractors.open_meteo_api_extractor import OpenMeteoExtractor
        
        logger.info("Starting weather extraction")
        retry_attempt = context["task_instance"].try_number - 1 # convert to zero-indexed

        extractor = OpenMeteoExtractor(
            latitude = location_config["latitude"],
            longitude = location_config["longitude"],
            location_name = location_config["location_name"],
            timezone = location_config["timezone"],
            hourly_variables = location_config.get("hourly_variables", ["temperature_2m"])
        )

        api_response, metadata = extractor.extract_forecast_data(
            retry_attempt = retry_attempt
        )

        logger.info(f"Extracted {location_config["location_name"]}")

        return {
            "api_response" : api_response,
            "metadata" : metadata,
            "location_name" : location_config["location_name"]
        }

    @task(outlets=[bronze_dataset])
    def load_city_to_bronze(extracted_data: dict) -> dict:
        """
        Use SQLExecutor class to connect psycopg2 to execute scripts locally.

        Args:
            extracted_data (dict): api_response, metadata, location_name

        Returns:
            dict: success (success status), location (location name)
        """
        from weather_etl.loaders.bronze_loader import SQLExecutor

        conn = psycopg2.connect(
            host = "postgres-warehouse",
            port = 5432,
            dbname = os.getenv("POSTGRES_WAREHOUSE_DB"),
            user = os.getenv("POSTGRES_WAREHOUSE_USER"),
            password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
        )

        try:
            sql_executor = SQLExecutor(conn)

            data_package = {
                "api_response" : extracted_data["api_response"],
                "metadata" : extracted_data["metadata"]
            }

            success = sql_executor.load_to_bronze(data_package)

            return {
                "success": success,
                "location": extracted_data["location_name"]
            }

        finally:
            conn.close()
    
    @task()
    def log_pipeline_result(load_result: dict):
        """
        Log the Pipeline result using logging module into PostgreSQL database for debugging.

        Args:
            load_result (dict): success (success status), location (location name)
        """
        import logging

        if load_result["success"]:
            logging.info(f"Pipeline completed for {load_result['location']}")
        else:
            logging.error(f"Pipeline failed for {load_result['location']}")

    # Specify one location (Bangkok) for now, may implement multi-city later
    bangkok_config = LOCATIONS[0]
    bangkok_config['hourly_variables'] = HOURLY_VARIABLES

    extracted = extract_city_weather_data(bangkok_config)
    loaded = load_city_to_bronze(extracted)
    log_pipeline_result(loaded)


weather_etl_pipeline()