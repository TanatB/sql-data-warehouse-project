import logging
from psycopg2.extras import Json


URL = "https://api.open-meteo.com/v1/forecast"

class DatabaseErrorLogger:
    """
    """
    def __init__(self, sql_executor, api_endpoint: str = URL):
        """
        Initialize error logger.

        Args:
            sql_executor:
            api_endpoint (str):
        """
        if not sql_executor:
            raise ValueError("sql_executor is required for DatabaseErrorLogger")

        self._sql_executor = sql_executor
        self._api_endpoint = api_endpoint

    def _log_api_error(
            self, 
            error_type: str, 
            error_message: str,
            latitude: float,
            longitude: float,
            http_status_code: int = None,
            request_params: dict = None,
            retry_attempt: int = 0
    ):
        """
        Log API errors to SQL bronze.api_error_log table

        Args:
            error_type (str): Type of error.
            error_message (str): Detailed error message
            latitude (float): Latitude of the ruquest
            longitude (float): Longitude of the request
            http_status_code (int): HTTP status code if applicable
            request_params (dict): API request parameters that caused the error
            retry_attempt (int): Current retry attempt number (0-indexed)
        """
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
                    self._api_endpoint,
                    error_type,
                    self._truncate_message(error_message),  # Truncated
                    http_status_code,
                    Json(request_params) if request_params else None,
                    latitude,
                    longitude,
                    retry_attempt
                )
            )
            logging.info(f"Error logged to database: {error_type}")
            return True

        except Exception as log_error:
            logging.error(
                f"❌ Failed to log error to the database table: {log_error}\n"
                f"Original error was: {error_type} - {error_message}"    
            )
            return False
    
    def _truncate_message(self, message: str, max_length: int = 1000) -> str:
        """
        """
        message_str = str(message)
        if len(message_str) <= max_length:
            return message_str
        
        return message[: max_length] + "..."