import psycopg2
import os
from dotenv import load_dotenv

from weather_etl.extractors.open_meteo_api_extractor import OpenMeteoExtractor
from weather_etl.extractors.extraction_orchestrator import ExtractionOrchestrator
from weather_etl.extractors.database_error_logger import DatabaseErrorLogger
from weather_etl.loaders.bronze_loader import SQLExecutor

load_dotenv()

def main():
    print("Hello from sql-data-warehouse-project!")


if __name__ == "__main__":
    main()
