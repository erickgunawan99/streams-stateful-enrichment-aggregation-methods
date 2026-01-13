from datetime import datetime, timezone
import json
import time
import signal
import sys
import random
from confluent_kafka import Producer
import threading

producer = Producer({
    'bootstrap.servers': 'localhost:29092'
})
stock_info = [
    {'symbol': 'AAPL', 'company': 'Apple Inc.', 'sector': 'Technology', 'market_cap': 2800000000000},
    {'symbol': 'GOOGL', 'company': 'Alphabet Inc.', 'sector': 'Technology', 'market_cap': 1700000000000},
    {'symbol': 'MSFT', 'company': 'Microsoft Corp.', 'sector': 'Technology', 'market_cap': 2900000000000},
    {'symbol': 'AMZN', 'company': 'Amazon.com Inc.', 'sector': 'Consumer Cyclical', 'market_cap': 1500000000000},
    {'symbol': 'TSLA', 'company': 'Tesla Inc.', 'sector': 'Automotive', 'market_cap': 760000000000},
    {'symbol': 'META', 'company': 'Meta Platforms Inc.', 'sector': 'Technology', 'market_cap': 850000000000},
    {'symbol': 'NVDA', 'company': 'NVIDIA Corp.', 'sector': 'Technology', 'market_cap': 1200000000000},
    {'symbol': 'JPM', 'company': 'JPMorgan Chase & Co.', 'sector': 'Financial Services', 'market_cap': 450000000000}
]

stop_event = threading.Event()

def update_market_caps():
   while not stop_event.is_set():
        stop_event.wait(5)  # Sleep but can be interrupted
        
        if stop_event.is_set():
            break
            
        info = random.choice(stock_info).copy()
        info['market_cap'] = int(info['market_cap'] * (1 + random.uniform(-0.01, 0.01)))
        info['info_timestamp'] = int(datetime.now(timezone.utc).timestamp() * 1000)
        producer.produce('stock-info', key=info['symbol'].encode('utf-8'), value=json.dumps(info).encode('utf-8'))
        producer.poll(0)
        print(f"Updated info: {info}")
        
def signal_handler(sig, frame):
    print('\nReceived shutdown signal...')
    stop_event.set()  # Signal thread to stop
    print('Flushing producer...')
    producer.flush()
    print('Shutdown complete.')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
print("Starting stock info producer...")


# Send initial data
for info in stock_info:
    info_with_time = info.copy()
    info_with_time['info_timestamp'] = int(datetime.now(timezone.utc).timestamp() * 1000)
    producer.produce('stock-info', key=info['symbol'].encode('utf-8'), value=json.dumps(info_with_time).encode('utf-8'))
    print(f"Sent info: {info_with_time}")
    time.sleep(0.5)
    producer.flush()

update_thread = threading.Thread(target=update_market_caps, daemon=False)
update_thread.start()

print("Initial data sent. Background updater running.")
print("Press Ctrl+C to stop.")