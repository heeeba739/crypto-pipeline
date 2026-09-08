from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os 
from dotenv import load_dotenv
load_dotenv()

default_args = {
    'owner': 'invistis',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1)
}

FRED_API_KEY =os.getenv("fred_api_key")

FRED_SERIES = {
    'FEDFUNDS': 'Federal Funds Rate',
    'CPIAUCSL': 'Consumer Price Index',
    'DGS10': '10-Year Treasury Rate',
    'UNRATE': 'Unemployment Rate',
    'GDP': 'Gross Domestic Product'
}

def fetch_fred_data():
    from confluent_kafka import Producer
    
    producer = Producer({
        'bootstrap.servers': ("CONFLUENT_BOOTSTRAP_SERVERS"),
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': ("CONFLUENT_API_KEY"),
        'sasl.password': ("CONFLUENT_API_SECRET")
    })
    
    for series_id, name in FRED_SERIES.items():
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json',
            'limit': 1,
            'sort_order': 'desc'
        }
        response = requests.get(url, params=params)
        obs = response.json().get('observations', [])
        
        if obs:
            record = {
                'series_id': series_id,
                'name': name,
                'value': obs[0]['value'],
                'date': obs[0]['date']
            }
            producer.produce(
                'fred_topic',
                key=series_id,
                value=json.dumps(record)
            )
            print(f" {name}: {obs[0]['value']}")
    
    producer.flush()

def store_to_data_lake():
    print(" Données FRED stockées dans le Data Lake")

with DAG(
    dag_id='fred_ingestion_dag',
    default_args=default_args,
    description='Ingestion quotidienne FRED API → Kafka → Data Lake',
    schedule_interval='@daily',
    catchup=False,
    tags=['invistis', 'fred', 'batch']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_fred_data',
        python_callable=fetch_fred_data
    )

    store_task = PythonOperator(
        task_id='store_to_data_lake',
        python_callable=store_to_data_lake
    )

    fetch_task >> store_task