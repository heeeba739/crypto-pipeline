import json
import time
import requests
from confluent_kafka import Producer
import os 
from dotenv import load_dotenv
load_dotenv()

# Config Kafka
producer = Producer({
    'bootstrap.servers':os.getenv ("CONFLUENT_BOOTSTRAP_SERVERS"),
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv("CONFLUENT_API_KEY"),
    'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
    'socket.timeout.ms': 60000,
    'message.timeout.ms': 60000,
    'batch.num.messages': 1
})

FRED_API_KEY = os.getenv("fred_api_key")  

FRED_SERIES = {
    'FEDFUNDS': 'Federal Funds Rate',
    'CPIAUCSL': 'Consumer Price Index',
    'DGS10': '10-Year Treasury Rate',
    'UNRATE': 'Unemployment Rate',
    'GDP': 'Gross Domestic Product'
}

def delivery_report(err, msg):
    if err:
        print(f' Kafka Error: {err}')
    else:
        print(f' Kafka OK → fred_topic offset={msg.offset()}')

def fetch_fred(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'limit': 1,
        'sort_order': 'desc'
    }
    response = requests.get(url, params=params)
    data = response.json()
    observations = data.get('observations', [])
    if observations:
        return observations[0]
    return None

def send_to_kafka():
    for series_id, name in FRED_SERIES.items():
        obs = fetch_fred(series_id)
        if obs:
            record = {
                'series_id': series_id,
                'name': name,
                'value': obs['value'],
                'date': obs['date']
            }
            print(f" {name}: {obs['value']} ({obs['date']})")
            producer.produce(
                'fred_topic',
                key=series_id,
                value=json.dumps(record),
                callback=delivery_report
            )
            producer.poll(5)
    producer.flush(timeout=15)

print(" FRED Producer démarré...")
while True:
    send_to_kafka()
    print(" Attente 24h...")
    time.sleep(86400)