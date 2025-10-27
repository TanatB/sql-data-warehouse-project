import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from datetime import datetime, timezone
import logging, json, os

from typing import Dict, List,Optional

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
    Designed for use in Apache Airflow DAGs.
    """
    def __init__(self, latitude: float, longitude: float, 
                 location_name: str, output_dir: str, 
                 timezone: str="auto",
                 hourly_variables: Optional[List[str]]=None,
                 forecast_days: int=7
    ):
        self.client = openmeteo_requests.Client()
        self._latitude = latitude
        self._longitude = longitude
        self._location_name = location_name # File Organization
        self._timezone = timezone
        self._output_dir = output_dir
        self._hourly_variables = hourly_variables
        self._forecast_days = forecast_days
        # self._use_cache = use_cache
        self._logger = logging.getLogger(__name__)
    
    # TODO
    def run(self):
        pass

    def extract_forecast_data(self):
        """
        Main entry point
        """
        try:
            request_time = self._get_request_timestamp()

            params = self._build_api_params()

            responses = self.client.weather_api(URL, params=params)
            response = responses[0]
            
            f"successfully extracted on: {request_time} (Local time)"

            return response
            
        except openmeteo_requests.OpenMeteoRequestsError as e:
            return f"failed to connect: {e}"

    # TODO
    def _handle_api_error(self, error):
        pass

    # Simple Methods
    def _build_api_params(self) -> Dict[str, any]:
        """
        5 parameters that are used to parse it to the API.
        """
        return {"latitude": self._latitude,
                "longitude": self._longitude,
                "timezone": self._timezone,
                "hourly": self._hourly_variables,
                "forecast_days": self._forecast_days
                }

    # TODO
    def _get_request_timestamp(self):
        return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # TODO
    def _generate_output_path(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp_str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        
        return 0

    # TODO: Core Methods
    def _add_metadata(self):
        create_at = ""
        extractor_version = ""
        generationtime_ms=0

        return 0

    # FIXME: need metadata
    def _parse_response(self, response):
        parsed_response = self._build_api_params()
        parsed_response.pop("hourly")
        
        parsed_response["timezone_abbreviation"] = response.TimezoneAbbreviation()
        parsed_response["utc_offset_seconds"] = response.UtcOffsetSeconds()
        parsed_response["elevation"] = response.Elevation()
        parsed_response["hourly"] = {}

        hourly = response.Hourly()

        hourly_variables = self._hourly_variables

        for variable_no in range(hourly.VariablesLength()):
            parsed_response["hourly"][hourly_variables[variable_no]] = hourly.Variables(variable_no).ValuesAsNumpy()

        return parsed_response
    
    # TODO: Storage
    def save_to_bronze(self):
        pass
    
    # TODO: Logging
    def _setup_logging(self):
        pass
    
    # TODO
    def log_extraction_metrics(self):
        pass
    
    # Optional
    def validate_data(self):
        pass

# TODO
class DataQualityValidator:
    pass


if __name__ == "__main__":
    print('Running the manual extractor script.')
    try:
        extractor = OpenMeteoExtractor(
            latitude=13.754, 
            longitude=100.5014, 
            location_name="Bangkok", 
            timezone="Asia/Bangkok", 
            output_dir=".",
            hourly_variables=[
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
    print(extractor._parse_response(response))
