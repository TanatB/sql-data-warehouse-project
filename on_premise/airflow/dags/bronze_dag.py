from airflow.sdk import DAG
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Import Python Classes from provided scripts
from weather_etl.extractors.open_meteo_api_extractor import OpenMeteoExtractor
from weather_etl.extractors import DatabaseErrorLogger, ExtractionOrchestrator

import logging
import os
from datetime import datetime, timedelta


LOCATIONS = [
    {
        "name": "Bangkok",
        "latitude": 13.754,
        "longitude": 100.5014,
        "timezone": "Asia/Bangkok"
    },

    {
        "name": "Tokyo",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "timezone": "Asia/Tokyo"
    },

    {
        "name": "Paris",
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
def bronze_weather_dag():
    """_summary_
    """

    @task()
    def get_location_configs() -> list:
        configs = []
        for loc in LOCATIONS:
            config = loc.copy()
            config["hourly_variables"] = HOURLY_VARIABLES
            configs.append(config)
        return configs


    @task(retries=3, retry_delay=timedelta(minutes = 2))
    def extract_city_weather_data(location_config: dict, **context) -> dict:
        """_summary_

        Args:
            location_config (dict): _description_

        Returns:
            dict: _description_
        """
        pass


    @task
    def load_city_to_bronze(extraction_result: dict):
        pass

    
    extractor = OpenMeteoExtractor(
            latitude = 13.754, 
            longitude = 100.5014, 
            location_name = "Bangkok", 
            timezone = "Asia/Bangkok",
            hourly_variables = [
            "temperature_2m", 
            "apparent_temperature", 
            "relative_humidity_2m", 
            "precipitation",
            "precipitation_probability", 
            "weather_code", 
            "wind_speed_10m", 
            "wind_direction_10m", 
            "surface_pressure", 
            "cloud_cover", 
            "rain", 
            "showers", 
            "snowfall", 
            "uv_index", 
            "is_day"
            ]
        )

dag_instance = bronze_weather_dag()