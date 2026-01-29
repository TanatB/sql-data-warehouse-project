from weather_etl.extractors.open_meteo_api_extractor import OpenMeteoExtractor
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
    """
    SQL Executor class used to execute SQL script from SQL directory
    
    Attributes:
        conn (): psycopg2 connection
    """

    def __init__(self, conn):
        self.conn = conn

    def execute_file(self, sql_file_path):
        """
        Overload the psycopg2 execute() method with our class method to run a whole script.
        
        Args:
            sql_file_path (str): path of the SQL script
        Returns:
            bool: True if success, False otherwise with Exceptions
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
    
    def execute_query(self, query):
        """
        Overload the psycopg2 execute() method with our class method to run a query.

        Args:
            query (str): SQL query in Python string.

        Returns:
            results (str): Query results
        
        Raises:
            Exception: if the query failed.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"Query failed: {e}")
            raise
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

    def load_to_bronze(self, data: dict):
        """
        Load raw response API data (in python dictionary format) to PostgreSQL database.
        
        Args:
            data (dict): api_response, metadata
        Returns:
            boolean: True or False 
        """
        cursor = self.conn.cursor()
        insert_query = """
                INSERT INTO bronze.weather_raw (
                location_name,
                latitude, longitude,
                timezone,
                api_retrieval_time, response_time_ms,
                created_at,
                raw_api_response)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cleaned_data = self._clean_format_in_dict(data)

            cursor.execute(insert_query, (
                    data["location_name"],
                    data["latitude"],
                    data["longitude"],
                    data["timezone"],
                    cleaned_data["metadata"]["api_retrieval_time"],
                    cleaned_data["metadata"]["response_time_ms"],
                    datetime.now(),
                    Json(cleaned_data["api_response"])
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


# MANUAL TEST
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
    sql_executor.execute_file(SQL_DIR / "01_bronze" / "bronze_layer.sql")
    sql_executor.load_to_bronze(raw_response)