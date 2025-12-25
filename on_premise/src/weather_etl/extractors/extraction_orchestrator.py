from open_meteo_api_extractor import OpenMeteoExtractor
from database_error_logger import DatabaseErrorLogger


class ExtractionOrchestrator:
    def __init__(self, 
                 extractor: OpenMeteoExtractor, 
                 logger: DatabaseErrorLogger = None):
        self._extractor = extractor
        self._logger = logger

    def execute_extraction(self):
        pass

    def _log_error(self):
        pass