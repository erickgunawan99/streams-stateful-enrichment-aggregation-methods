import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

# Kafka configuration
producer = Producer({
    'bootstrap.servers': 'localhost:29092'
})

stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM']
base_prices = {
    'AAPL': 178.0, 'GOOGL': 140.0, 'MSFT': 370.0, 'AMZN': 145.0,
    'TSLA': 240.0, 'META': 330.0, 'NVDA': 480.0, 'JPM': 150.0
}

def generate_trade():
    symbol = random.choice(stocks)
    base_price = base_prices[symbol]
    price = round(base_price * (1 + random.uniform(-0.02, 0.02)), 2)
    volume = random.randint(10, 1000)
    
    return {
        'symbol': symbol,
        'price': price,
        'volume': volume,
        'trade_timestamp': int(datetime.now(timezone.utc).timestamp() * 1000)
    }

print("Starting stock trades producer...")
try:
    while True:
        trade = generate_trade()
        producer.produce('stock-trades', key=trade['symbol'].encode('utf-8'), value=json.dumps(trade).encode('utf-8'))
        print(f"Sent trade: {trade}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down producer...")
finally:
    producer.flush()