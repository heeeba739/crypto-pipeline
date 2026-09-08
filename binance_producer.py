import json
import time
import threading
import websocket
from confluent_kafka import Producer
import os
from dotenv import load_dotenv
load_dotenv()

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

SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "adausdt"]
stream = "/".join([f"{s}@trade" for s in SYMBOLS])
socket = f"wss://stream.binance.com:9443/stream?streams={stream}"

latest_trades = {}
lock = threading.Lock()

def delivery_report(err, msg):
    if err:
        print(f' Kafka Error: {err}')
    else:
        print(f' Kafka OK → {msg.topic()} offset={msg.offset()}')

def on_message(ws, message):
    msg = json.loads(message)
    data = msg["data"]
    trade = {
        'symbol': data['s'],
        'price': float(data['p']),
        'quantity': float(data['q']),
        'timestamp': data['T'],
        'trade_id': data['t']
    }
    print(f" {trade['symbol']} | Price: {trade['price']:.2f} | Qty: {trade['quantity']}")
    with lock:
        latest_trades[trade['symbol']] = trade

def on_open(ws):
    print(" Connecté à Binance — streaming 5 symbols...")

def on_error(ws, error):
    print(f" WS Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(" Connexion fermée")

def kafka_thread():
    while True:
        time.sleep(3)
        with lock:
            trades = dict(latest_trades)
            latest_trades.clear()
        for symbol, trade in trades.items():
            producer.produce(
                'trades_topic',
                key=symbol,
                value=json.dumps(trade),
                callback=delivery_report
            )
            producer.poll(5)

t = threading.Thread(target=kafka_thread, daemon=True)
t.start()

ws = websocket.WebSocketApp(
    socket,
    on_message=on_message,
    on_open=on_open,
    on_error=on_error,
    on_close=on_close
)
ws.run_forever()