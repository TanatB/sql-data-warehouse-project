from airflow.sdk import dag, task
from airflow.datasets import Dataset

import psycopg2

import os, logging
from datetime import datetime, timedelta

from pathlib import Path


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR = PROJECT_ROOT / "sql"

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
        "timezone": "Europe/Paris"
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
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes = 30),
}

# URI is just a convention (can be anything) but I use this for self-documenting
bronze_dataset = Dataset("bronze_weather_raw")

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
    @task
    def get_locations():
        """
        Return list of locations to extract with default hourly_variables included.

        Returns:
            dict: location_name, latitude, longitude, timezone, hour_variables
        """
        return [
            {**location, "hourly_variables": HOURLY_VARIABLES}
            for location in LOCATIONS
        ]

    @task(retries=3, retry_delay=timedelta(minutes = 1))
    def extract_city_weather_data(location_config: dict, **context) -> dict:
        """
        Extract weather data from Open-Meteo API.

        Args:
            location_config (dict): latitude, longtitude, location_name, timezone

        Returns:
            dict: api_response & metadata

        Raises:
            Exception: if the location is failed to extract
        """
        from weather_etl.extractors.open_meteo_api_extractor import OpenMeteoExtractor

        location_name = location_config["location_name"]
        
        logger.info(f"Starting weather extraction for city: {location_name}")
        retry_attempt = context["task_instance"].try_number - 1 # convert to zero-indexed
        try:
            extractor = OpenMeteoExtractor(
                latitude = location_config["latitude"],
                longitude = location_config["longitude"],
                location_name = location_name,
                timezone = location_config["timezone"],
                hourly_variables = location_config.get("hourly_variables", HOURLY_VARIABLES)
            )

            api_response, metadata = extractor.extract_forecast_data(
                retry_attempt = retry_attempt
            )

            logger.info(f"Succesfully extracted {location_name}")

            return {
                "location_name" : location_name,
                "latitude": location_config["latitude"],
                "longitude": location_config["longitude"],
                "timezone" : location_config["timezone"],
                "api_response" : api_response,
                "metadata" : metadata
            }
        
        except Exception as e:
            logger.error(f"Failed to extract {location_name}: {e}")
            raise

    @task(outlets=[bronze_dataset])
    def load_city_to_bronze(extracted_data: dict) -> dict:
        """
        Use SQLExecutor class to connect psycopg2 to execute SQL scripts locally.

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
            sql_executor.execute_file(SQL_DIR / "01_bronze" / "bronze_layer.sql")
            logging.info("Bronze schema/table Initialized.")
            success = sql_executor.load_to_bronze(extracted_data)
            
            return {
                "location": extracted_data["location_name"],
                "status": success
            }
            
        except Exception as e:
            logger.error(f"Bronze extraction failed: {e}")
            raise

        finally:
            conn.close()
    
    @task(trigger_rule="all_done")
    def log_pipeline_result(load_results: list):
        """
        Log the Pipeline result using logging module into PostgreSQL database for debugging.

        Args:
            load_result (dict): success (success status), location (location name)
        """

        successful = [result for result in load_results if result is not None]
        failed_count = len(LOCATIONS) - len(successful)

        logger.info(f"Extraction complete: {len(successful)} succeed, {failed_count} failed.")

        if failed_count > 0:
            logger.warning("Some locations failed Extraction")

            return {
                "successful": len(successful),
                "failed": failed_count,
                "details": successful
            }


    # Single City
    # bangkok_config = LOCATIONS[0]
    # bangkok_config['hourly_variables'] = HOURLY_VARIABLES

    locations = get_locations()

    extracted = extract_city_weather_data.expand(location_config=locations)

    loaded = load_city_to_bronze.expand(extracted_data=extracted)

    log_pipeline_result(loaded)


dag = weather_etl_pipeline()