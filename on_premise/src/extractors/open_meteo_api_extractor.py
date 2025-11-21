import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from datetime import datetime, timezone
import logging

from typing import Dict, List, Optional, Tuple, Any
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
        Extract weather forecast data from Open-Meteo API.
        
        Attributes:
            latitude (float): Latitude of the location
            longitude (float): Longitude of the location
            location_name (str): location name for organization
            timezone (str): Timezone e.g. 'Asia/Bangkok'
            hourly_variables (list[str], optional): List of hourly variables to request
            forecast_days (int): Numbers of days to forecast
        """
    def __init__(
            self, 
            latitude: float, 
            longitude: float, 
            location_name: str, 
            timezone: str = "GMT",
            hourly_variables: Optional[List[str]] = None,
            forecast_days: int = 7
    ):
        """
        Initialize OpenMeteo API Extractor.

        Args:
            latitude (float):
            longitude (float):
            location_name (str):
            timezone (str):
            hourly_variables (list[str], optional):
            forecast_days (int):
        """
        self._latitude = latitude
        self._longitude = longitude
        self._location_name = location_name # File Organization
        self._timezone = timezone
        self._hourly_variables = hourly_variables or self._get_default_hourly_variables()
        self._forecast_days = forecast_days
        
        # Client Setup
        self._client = self._setup_client()

    def _setup_client(self) -> openmeteo_requests.Client:
        """
        """
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        return openmeteo_requests.Client(session=retry_session)
    
    def _get_default_hourly_variables(self) -> List[str]:
        """
        Get default list of hourly variables (only temperature) if none provided.

        Returns:
            List[str]: Default weather variables
        """
        return [
            "temperature_2m"
        ]

    # TODO: separate error handling from this function
    def extract_forecast_data(self, retry_attempt: int = 0):
        """
        Extract weather forecast data from Open-Meteo API.

        Args:
            retry_attempt (int): Current retry attempt number (passed by Airflow)

        Returns:
            tuple[Dict, Dict]: (api_response, metadata)
                - api_response: Parsed weather data
                - metadata: Extraction metadata (timestamp, response time)

        Raises:
            OpenMeteoRequestsError: API request failed
            ValueError: Invalid response data
            Exception: Unexpected errors
        """
        request_start = datetime.now(timezone.utc)

        params = self._build_api_params()

        # Make API request
        responses = self.client.weather_api(URL, params=params)
        response_time_ms = self._calculate_response_time(request_start)

        self._validate_response(responses)

        parsed_data = self._parse_response(responses[0])

        self._validate_parsed_data(parsed_data)

        cleaned_data = self._clean_response_data(parsed_data)

        logging.info(f"✅ Successfully extracted data for {self._location_name}"
                     f"(response time: {response_time_ms} ms)")
            
        metadata = self._build_metadata(request_start, response_time_ms)

        return cleaned_data, metadata
            
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
    
    # TODO
    def _build_metadata(self, request_start: datetime, response_time_ms: float) -> Dict[str, Any]:
        """
        """
        return {
            "api_retrieval_time": request_start,
            "response_time_ms": response_time_ms
        }

    def _parse_response(self, response) -> Dict[str, Any]:
        """
        Parse Open-Meteo API response into structured dictionary.
        
        Args:
            response : Open-Meteo API response Object
        Returns:
            Dict[str, Any]: parsed weather response
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
        time_range = self._extract_time_range(hourly)
        parsed_response["hourly"]["time"] = time_range.tolist()

        # Manually add each response to dictionary.
        for variable_no in range(hourly.VariablesLength()):
            parsed_response["hourly"][hourly_variables[variable_no]] = hourly.Variables(variable_no).ValuesAsNumpy().tolist()

        return parsed_response

    def _calculate_response_time(self, request_start: datetime) -> float:
        """
        Calculate the API response time in milliseconds.
        
        Args:
            request_start (datetime): datetime format (UTC)

        Returns:
            float: response time in milliseconds (2 decimal points)
        """
        return round(
            (datetime.now(timezone.utc) - request_start).total_seconds() * 1000, 
            2
        )
    
    def _extract_time_range(self, hourly) -> pd.DatetimeIndex:
        """
        Extract time range from hourly data.

        Args:
            hourly: Hourly data object generated from API response
        
        Returns:
            pd.DatetimeIndex: Time range for hourly data
        """
        return pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

    # TODO: Error Handlers
    def _validate_response(self, responses: List) -> None:
        """
        Validate that API returned a non-empty response.

        Args:
            response (List): API response list
        
        Raises:
            ValueError: If response is empty or None
        """
        if not responses or len(responses) == 0:
            raise ValueError("API returned empty response")

    def _validate_parsed_data(self, parsed_data: Dict[str, Any]) -> None:
        """
        Validate that parsed data contains require fields.

        Args:
            parse_data (Dict): Parsed API response

        Raises:
            ValueError: If required fields are missing
        """
        if "hourly" not in parsed_data:
            raise ValueError("Missing 'hourly' data in API response")
        
        if not parsed_data(['hourly']):
            raise ValueError("'hourly' data is empty in API response")

    def _clean_response_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean response data (e.g. decode bytes fields).

        Args:
            data (Dict[Str, Any]): 
        
        Return data (Dict[Str, Any]):
        """
        if 'timezone_abbreviation' in data and isinstance(data['timezone_abbreviation'], bytes):
            data['timezone_abbreviation'] = data['timezone_abbreviation'].decode('utf-8')

        return data

    # Properties
    @property
    def location_name(self) -> str:
        return self._location_name
    
    @property
    def coordinates(self) -> Tuple[float, float]:
        return (self._latitude, self._longitude)
    

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