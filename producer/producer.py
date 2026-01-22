import time
import json
import requests
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "crypto_prices"
API_URL = "https://api.coingecko.com/api/v3/simple/price"

# Wait for Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(3, 4, 0)
        )
        print("Connected to Kafka", flush=True)
        break
    except Exception as e:
        print("Waiting for Kafka...", e, flush=True)
        time.sleep(5)

while True:
    try:
        response = requests.get(
            API_URL,
            params={"ids": "bitcoin,ethereum", "vs_currencies": "usd"},
            timeout=10
        )

        data = response.json()

        if "bitcoin" not in data or "ethereum" not in data:
            print("Invalid API response:", data)
            time.sleep(60)
            continue

        message = {
            "btc_price": data["bitcoin"]["usd"],
            "eth_price": data["ethereum"]["usd"],
            "timestamp": int(time.time())
        }

        producer.send(TOPIC, message)
        producer.flush()
        print("Sent:", message, flush=True)

    except Exception as e:
        print("Producer error:", e, flush=True)

    time.sleep(60)  # avoid API rate limit
