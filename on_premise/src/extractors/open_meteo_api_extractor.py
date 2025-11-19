import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from datetime import datetime, timezone
import pytz
import logging, json, os

from typing import Dict, List,Optional
import psycopg2
from psycopg2.extras import Json

# NOTE: Referenced parameters
PARAMS = {
    "latitude": 13.754,
	"longitude": 100.5014,
	"hourly": [
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
    ],
	"timezone": "Asia/Bangkok",
}

URL = "https://api.open-meteo.com/v1/forecast"


"""
JSON Return Object:

{
    "latitude": 52.52,
    "longitude": 13.419,
    "elevation": 44.812,
    "generationtime_ms": 2.2119,
    "utc_offset_seconds": 0,
    "timezone": "Europe/Berlin",
    "timezone_abbreviation": "CEST",
    "hourly": {
        "time": ["2022-07-01T00:00", "2022-07-01T01:00", "2022-07-01T02:00", ...],
        "temperature_2m": [13, 12.7, 12.7, 12.5, 12.5, 12.8, 13, 12.9, 13.3, ...],
        ...
    },
    "hourly_units": {
        "temperature_2m": "°C"
    }
}
"""


class OpenMeteoExtractor:
    """
        Designed for use in Apache Airflow.
        
        Attributes:
            latitude (float): Latitude of the location
            longitude (float): Longitude of the location
            location_name (str): location name convention for devs
            output_dir (str):
            timezone (str): Timezone e.g. 'Asia/Bangkok'
            hourly_variables (list[str], optional): List of hourly variables
            forecast_days (int): Numbers of days forecasted
            sql_executor (class): Executor Class from bronze_loader.py
        """
    def __init__(self, latitude: float, longitude: float, 
                 location_name: str, output_dir: str, 
                 timezone: str = "GMT",
                 hourly_variables: Optional[List[str]] = None,
                 forecast_days: int = 7,
                 sql_executor = None
    ):
        """
        Test.

        Args:
            latitude (float):
            longitude (float):
            location_name (str):
            output_dir (str):
            timezone (str):
            hourly_variables (list[str], optional):
            forecast_days (int):
        """
        self._latitude = latitude
        self._longitude = longitude
        self._location_name = location_name # File Organization
        self._timezone = timezone
        self._output_dir = output_dir
        self._hourly_variables = hourly_variables
        self._forecast_days = forecast_days
        self._logger = logging.getLogger(__name__)
        self._sql_executor = sql_executor
        
        # Client Setup
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        self.client = openmeteo_requests.Client(session=retry_session)

    # TODO: add more exception catches & log them into the SQL table.
    def extract_forecast_data(self, retry_attempt: int = 0):
        """
        Extract weather forecast data from Open-Meteo API.

        Args:
            retry_attempt (int): Current retry attempt number (passed by Airflow)

        Returns:
            tuple: (api_response_dictionary, metadata_dictionary)

        Raises:
            OpenMeteoRequestsError: API request failed
            ValueError: Invalid response data
            Exception: Unexpected errors
        """
        request_start = datetime.now(timezone.utc)
        params = None

        try:
            params = self._build_api_params()

            # Make API request
            responses = self.client.weather_api(URL, params=params)
            response_time_ms = round(
                (datetime.now(timezone.utc) - request_start).total_seconds() * 1000, 
                2
            )

            if not responses or len(responses) == 0:
                raise ValueError("API returned empty response.")

            response = responses[0]

            parsed_data = self._parse_response(response)

            if 'hourly' not in parsed_data:
                raise ValueError("Missing 'hourly' data in API response.")

            # Decode bytes fields
            if 'timezone_abbreviation' in parsed_data:
                if isinstance(parsed_data['timezone_abbreviation'], bytes):
                    parsed_data['timezone_abbreviation'] = parsed_data['timezone_abbreviation'].decode('utf-8')

            logging.info(f"✅ Successfully extracted data, response time: {response_time_ms} ms")
            
            metadata = {
                    "api_retrieval_time": request_start,
                    "response_time_ms": response_time_ms
                }

            return parsed_data, metadata
            
        except openmeteo_requests.OpenMeteoRequestsError as e:
            response_time_ms = round(
                (datetime.now(timezone.utc) - request_start).total_seconds() * 1000, 
                2
            )

            error_msg = f"OpenMeteo API request failed: {str(e)}"
            logging.error(f" {error_msg} (response time: {response_time_ms} ms)")
            
            # Log to error table if sql_executor is available
            if hasattr(self, '_sql_executor') and self._sql_executor:
                pass

            raise   # Re-raise for Airflow to handle

        except ValueError as e:
            error_msg = f"Invalid API response: {str(e)}"
            logging.error(f" {error_msg}")
            
            raise

        except Exception as e:
            error_msg = f"Unexpected error during extraction: {str(e)}"
            logging.error(f" {error_msg}")
            
            raise

    # Simple Methods
    def _build_api_params(self) -> Dict[str, any]:
        """
        5 parameters that are used to parse it to the API.
        
        Returns:
            Dict[str, any]:
        """
        return {"latitude": self._latitude,
                "longitude": self._longitude,
                "timezone": self._timezone,
                "hourly": self._hourly_variables,
                "forecast_days": self._forecast_days
                }

    def _get_request_timestamp(self):
        """
        test.

        Returns:
            datetime: current date and time format.
        """
        return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # Optional: maybe implement this for unit test.
    def _generate_output_path(self):
        """
        test.

        Returns:
            int: 0
        """
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        timestamp_str = now.strftime("%Y-%m-%dT%H-%M-%S")
        
        return 0

    def _parse_response(self, response):
        """
        test.
        
        Args:
            response (dict):
        Returns:
            dict: parsed response
        """
        parsed_response = self._build_api_params()
        parsed_response.pop("hourly")
        
        parsed_response["timezone_abbreviation"] = response.TimezoneAbbreviation()
        parsed_response["utc_offset_seconds"] = response.UtcOffsetSeconds()
        parsed_response["elevation"] = response.Elevation()
        parsed_response["hourly"] = {}

        hourly = response.Hourly()

        hourly_variables = self._hourly_variables

        # time period (UTC time)
        time_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        parsed_response["hourly"]["time"] = time_range.tolist()

        # Manually add each response to dictionary.
        for variable_no in range(hourly.VariablesLength()):
            parsed_response["hourly"][hourly_variables[variable_no]] = hourly.Variables(variable_no).ValuesAsNumpy().tolist()

        return parsed_response
    
    # TODO: API error logging
    def _log_api_error(
            self, 
            error_type: str, 
            error_message: str,
            http_status_code: int = None,
            request_params: dict = None,
            retry_attempt: int = 0
    ):
        """
        Log API errors to SQL bronze.api_error_log table

        Args:
            error_type (str): Type of error.
            error_message (str): Detailed error message.
            http_status_code (int): HTTP status code if applicable.
            request_params (dict): API request parameters that caused the error.
            retry_attempt (int): Current retry attempt number (0-indexed).
        """

        if not self._sql_executor:
            logging.warning(f"Cannot log error to database: no sql_executor configured")
            return

        insert_query = """--sql
            INSERT INTO bronze.api_error_log (
                api_endpoint,
                error_type,
                error_message,
                http_status_code,
                request_params,
                latitude,
                longitude,
                retry_attempt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            self._sql_executor.execute_query(
                insert_query,
                (
                    self._base_url,
                    error_type,
                    str(error_message)[:1000],  # Truncated
                    http_status_code,
                    Json(request_params) if request_params else None,
                    self._latitude,
                    self._longitude,
                    retry_attempt
                )
            )
            logging.info(f"Error logged to database: {error_type}")

        except Exception as log_error:
            logging.error(
                f"failed to log error to the database table: {log_error}\n"
                f"Original error was: {error_type} - {error_message}"    
            )


if __name__ == "__main__":
    # Testing the script
    print('Running the manual extractor script.')
    try:
        extractor = OpenMeteoExtractor(
            latitude = 13.754, 
            longitude = 100.5014, 
            location_name = "Bangkok", 
            timezone = "Asia/Bangkok", 
            output_dir = ".",
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
        print("Class instance created.")
    except Exception as e:
        print(f"Message: {e}")
    
    response = extractor.extract_forecast_data()

    print(response)