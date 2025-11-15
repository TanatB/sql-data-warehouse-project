from on_premise.src.extractors.open_meteo_api_extractor import OpenMeteoExtractor
import psycopg2
from psycopg2.extras import Json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
SQL_DIR = PROJECT_ROOT / "on_premise" / "sql"

# Load environment variables from .env file
load_dotenv()

host = "localhost"
port= os.getenv("POSTGRES_WAREHOUSE_PORT")
db_name = os.getenv("POSTGRES_WAREHOUSE_DB")
user = os.getenv("POSTGRES_WAREHOUSE_USER")
password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")


class SQLExecutor:
    """Test
    Args:
        conn ():
    
    Attributes:
        conn ():
    """

    def __init__(self, conn):
        self.conn = conn

    def execute_file(self, sql_file_path):
        """
        Recursively convert bytes to strings in nested dict/list structures.
        
        Args:
            sql_file_path (str):
        Returns:
            bool: 
        """
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()

        cursor = self.conn.cursor()
        
        try:
            cursor.execute(sql_script)
            self.conn.commit()
            print(f"Executed: {sql_file_path}")
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"Failed: {sql_file_path}")
            print(f" Error: {e}")
            return False
        finally:
            cursor.close()

    def execute_directory(self, sql_dir, pattern="*.sql"):
        """
        test.
        
        Args:
            sql_dir (str):
            pattern (str):
        Returns:
            object: 
        """
        pass

    def convert_to_json(self):
        """
        test.
        
        """
        pass

    def insert_json(self, data):
        """
        Recursively convert bytes to strings in nested dict/list structures.
        
        Args:
            obj (object)
        Returns:
            object: 
        """
        cursor = self.conn.cursor()
        insert_query = """--sql
                INSERT INTO bronze.weather_raw (
                latitude, longitude, api_retrieval_time,
                raw_response, response_time_ms, forecast_days, 
                ingestion_timestamp, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cleaned_data = self._clean_format_in_dict(data)

            cursor.execute(insert_query, (
                    cleaned_data["api_response"]["latitude"],
                    cleaned_data["api_response"]["longitude"],
                    cleaned_data["metadata"]["api_retrieval_time"],
                    Json(cleaned_data),
                    cleaned_data["metadata"]["response_time_ms"],
                    7,
                    datetime.now(),
                    datetime.now()
            ))

            self.conn.commit()
            print("successfully inserted the data.")

            return True
        
        except Exception as e:
            self.conn.rollback()
            print(f" Error: {e}")

            return False
        
        finally:
            cursor.close()

    def _clean_format_in_dict(self, obj):
        """
        Recursively convert bytes/datetime format to strings in nested dict/list.

        Args:
            obj (object):
        
        Returns:
            obj:
        """
        if isinstance(obj, dict):
            return {k: self._clean_format_in_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_format_in_dict(item) for item in obj]
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        elif isinstance(obj, (datetime)):
            return obj.isoformat()
        else:
            return obj


if __name__ == "__main__":
    extractor = OpenMeteoExtractor(latitude=13.754, 
                longitude=100.5014, 
                location_name="Bangkok", 
                timezone="Asia/Bangkok", 
                output_dir=".",
                hourly_variables=[
                "temperature_2m"
                ])
    raw_response = extractor.extract_forecast_data()

    print(type(raw_response))
    # print(raw_response)

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db_name,
        user=user,
        password=password
    )
    sql_executor = SQLExecutor(conn)
    # sql_executor.execute_file(SQL_DIR / "00_init" / "init_db.sql")
    # sql_executor.execute_file(SQL_DIR / "01_bronze" / "bronze_layer.sql")
    sql_executor.insert_json(raw_response)