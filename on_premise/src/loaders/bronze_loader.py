from on_premise.src.extractors.open_meteo_api_extractor import OpenMeteoExtractor
import psycopg2
import os
from dotenv import load_dotenv

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
print(extractor)

host = "localhost"
db_name = os.getenv("POSTGRES_DB")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")

# Connection to the database
conn = psycopg2.connect(host=host, port=5433, 
                        dbname=db_name, 
                        user=user,
                        password=password)

conn.autocommit = True

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute command
cur.execute("SELECT * FROM bronze.weather_raw;")

# Close the communication with the database
cur.close()
conn.close()