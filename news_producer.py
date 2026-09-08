import json
import time
import requests
from confluent_kafka import Producer
import os
from dotenv import load_dotenv
load_dotenv()

# Config Kafka
producer = Producer({
    'bootstrap.servers': os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"),
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv("CONFLUENT_API_KEY"),
    'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
    'socket.timeout.ms': 60000,
    'message.timeout.ms': 60000,
    'batch.num.messages': 1
})

NEWS_API_KEY = os.getenv("news_api_key")  

def delivery_report(err, msg):
    if err:
        print(f' Kafka Error: {err}')
    else:
        print(f' Kafka OK → news_topic offset={msg.offset()}')

def fetch_news():
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)",
        "sources": "reuters,bloomberg,cnbc,cointelegraph,financial-times",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 5,
        "apiKey": NEWS_API_KEY
    }
    response = requests.get(url, params=params)
    return response.json().get("articles", [])

def send_to_kafka(articles):
    for article in articles:
        news = {
            'title': article.get('title'),
            'source': article.get('source', {}).get('name'),
            'published_at': article.get('publishedAt'),
            'description': article.get('description'),
            'url': article.get('url')
        }
        producer.produce(
            'news_topic',
            key=news['source'] or 'unknown',
            value=json.dumps(news),
            callback=delivery_report
        )
        producer.poll(5)
    producer.flush(timeout=15)

# Boucle principale — fetch toutes les 60 secondes
print(" News Producer démarré...")
while True:
    print(" Fetch articles...")
    articles = fetch_news()
    print(f" {len(articles)} articles récupérés")
    send_to_kafka(articles)
    print(" Attente 60 secondes...")
    time.sleep(60)