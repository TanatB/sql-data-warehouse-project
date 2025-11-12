from on_premise.src.extractors.open_meteo_api_extractor import OpenMeteoExtractor
import psycopg2
from psycopg2.extras import Json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

host = "localhost"
db_name = os.getenv("POSTGRES_DB")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")


class SQLExecutor:
    def __init__(self, conn):
        self.conn = conn

    def execute_file(self, sql_file_path):
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
        pass

    def convert_to_json(self):
        pass

    def insert_json(self, data):
        cursor = self.conn.cursor()
        insert_query = """--sql
                INSERT INTO bronze.weather_raw (
                latitude, longitude, api_retrieval_time, request_id, 
                raw_response, response_time_ms, forecast_days, 
                ingestion_timestamp, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor.execute(insert_query, (
                    data["api_response"]["latitude"],
                    data["api_response"]["longitude"],
                    data["metadata"]["api_retrieval_time"],
                    Json(data),
                    data["metadata"]["response_time_ms"],
                    7,
                    datetime.now(),
                    datetime.now()
            ))

            print("successfully inserted the data.")

            return True
        
        except Exception as e:
            self.conn.rollback()
            print(f" Error: {e}")

            return False
        
        finally:
            cursor.close()


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    extractor = OpenMeteoExtractor(latitude=13.754, 
                longitude=100.5014, 
                location_name="Bangkok", 
                timezone="Asia/Bangkok", 
                output_dir=".",
                hourly_variables=[
                "temperature_2m"
                ])
    raw_response = extractor.extract_forecast_data()
    # print(raw_response)

    # conn = psycopg2.connect(
    #     host=host,
    #     database=db_name,
    #     user=user,
    #     password=password
    # )
    # sql_executor = SQLExecutor(conn)