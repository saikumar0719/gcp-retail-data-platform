"""Kafka -> Pub/Sub bridge.

Reads events from local Kafka topics and republishes them to GCP Pub/Sub,
where the Dataflow streaming pipeline picks them up. This keeps Kafka as the
ingestion front door while Dataflow stays on its native Pub/Sub source.

    python streaming/kafka_to_pubsub.py --project my-gcp-project
    python streaming/kafka_to_pubsub.py --dry-run   # print, don't publish
"""

import argparse
import json

from kafka import KafkaConsumer

TOPIC_MAP = {
    "retail-orders": "retail-orders",
    "retail-clicks": "retail-clicks",
    "retail-inventory": "retail-inventory",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--project", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    publisher = None
    if not args.dry_run:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()

    consumer = KafkaConsumer(
        *TOPIC_MAP.keys(),
        bootstrap_servers=[args.bootstrap],
        # One consumer group: add more consumers to scale, Kafka rebalances
        # partitions across them. Offsets commit only after publish.
        group_id="retail-pubsub-bridge",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for message in consumer:
        topic = TOPIC_MAP[message.topic]
        payload = json.dumps(message.value).encode("utf-8")
        if args.dry_run:
            print(f"[dry-run] {message.topic} -> {topic}: {payload[:120]}")
        else:
            future = publisher.publish(
                f"projects/{args.project}/topics/{topic}",
                payload,
                event_id=message.value["event_id"],
            )
            future.result()  # raise on publish failure before committing
        consumer.commit()


if __name__ == "__main__":
    main()
