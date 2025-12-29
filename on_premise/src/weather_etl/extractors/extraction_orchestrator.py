import logging
from typing import Optional, Dict, Any

from weather_etl.extractors.open_meteo_api_extractor import OpenMeteoExtractor
from weather_etl.extractors.database_error_logger import DatabaseErrorLogger


class ExtractionOrchestrator:
    """
    Docstring for ExtractionOrchestrator
    """

    def __init__(
        self, 
        extractor: OpenMeteoExtractor,
        sql_executor, 
        error_logger: Optional[DatabaseErrorLogger] = None
    ):
        """_summary_

        Args:
            extractor (OpenMeteoExtractor): _description_
            sql_executor (_type_): _description_
            error_logger (Optional[DatabaseErrorLogger], optional): _description_. Defaults to None.
        """
        self._extractor = extractor
        self._sql_executor = sql_executor
        self._error_logger = error_logger
        self._logger = logging.getLogger(__name__)

    def execute_extraction(self, retry_attempt: int = 0) -> Dict[str, Any]:
        """_summary_

        Args:
            retry_attempt (int, optional): _description_. Defaults to 0.

        Returns:
            Dict[str, Any]: _description_
        """
        location = self._extractor.location_name
        self._logger.info(f" Starting extraction for {location}")

        try:
            api_response, metadata = self._extractor.extract_forecast_data(
                retry_attempt = retry_attempt
            )
            self._logger.info(f"Extracted data for {location}")

            data_package = {
                "api_response" : api_response,
                "metdata" : metadata
            }

            success = self._sql_executor.load_to_bronze(data_package)

            if success:
                self._logger(f"Loaded data to Bronze Layer")
                return {
                    "success" : True,
                    "location" : location,
                    "response_time_ms" : metadata["response_time_ms"]
                }
            else:
                raise Exception("load_to_bronze returned False")

        except Exception as e:
            self._handle_error(e, retry_attempt)
            return {
                "success" : False,
                "location" : location,
                "error" : str(e)
            }

    def _handle_error(self, error: Exception, retry_attempt: int) -> None:
        """_summary_

        Args:
            error (Exception): _description_
            retry_attempt (int): _description_
        """
        error_type = type(error).__name__
        error_message = str(error)

        self._logger.error(
            f"❌ Extraction failed for {self._extractor.location_name}: "
            f"{error_type} - {error_message}"
        )
        
        if self._error_logger:
            lat, lon = self._extractor.coordinates
            self._error_logger._log_api_error(
                error_type=error_type,
                error_message=error_message,
                latitude=lat,
                longitude=lon,
                retry_attempt=retry_attempt,
                request_params=self._extractor._build_api_params()
            )