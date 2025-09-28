from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data

url = ""

default_args = {
    'owner' : '',
}


with DAG('user_automation',
         ) as dag:
    
    streaming_task = PythonOperator(
        print('hello')
    )


if __name__ == '__main__':
    print('test123')