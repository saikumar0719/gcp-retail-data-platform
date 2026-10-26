"""Simulated e-commerce event producer -> Kafka.

Generates order, click, and inventory events and publishes them to Kafka
topics. Run against the local Docker stack in docker-compose.yml:

    docker compose -f producers/docker-compose.yml up -d
    python producers/kafka_producer.py --events 1000
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPICS = ["retail-orders", "retail-clicks", "retail-inventory"]
PRODUCTS = [f"SKU-{i:04d}" for i in range(1, 51)]
CUSTOMERS = [f"CUST-{i:05d}" for i in range(1, 501)]


def make_event() -> dict:
    event_type = random.choice(["order", "click", "inventory"])
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "customer_id": random.choice(CUSTOMERS),
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5) if event_type == "order" else None,
        "amount": round(random.uniform(9.99, 499.99), 2)
        if event_type == "order"
        else None,
    }
    return event


def topic_for(event: dict) -> str:
    return {
        "order": "retail-orders",
        "click": "retail-clicks",
        "inventory": "retail-inventory",
    }[event["event_type"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--delay-ms", type=int, default=10)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Keying by product keeps all events for one product ordered
        # within a partition.
        key_serializer=lambda k: k.encode("utf-8"),
    )
    for _ in range(args.events):
        event = make_event()
        producer.send(topic_for(event), key=event["product_id"], value=event)
        time.sleep(args.delay_ms / 1000.0)
    producer.flush()
    print(f"Published {args.events} events to Kafka")


if __name__ == "__main__":
    main()
