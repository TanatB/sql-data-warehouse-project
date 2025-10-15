from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data

url = ""

default_args = {
    'owner' : 'tanatb',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

## DAG
with DAG(dag_id='elt_automation', start_date=datetime(2025, 10, 1),
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        """ Extract weather data from the API."""
        pass

    @task()
    def load_to_postgres():
        """
        LOAD
        """
        pass
    
    @task()
    def clean_data():
        """
        CLEAN (Silver Layer)
        """
        pass

    @task()
    def transform_data():
        """
        Aggregate (Gold)
        """
        pass
    


if __name__ == '__main__':
    extract_weather_data() >> load_to_postgres() >> clean_data() >> transform_data()