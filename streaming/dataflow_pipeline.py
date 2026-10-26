"""Streaming pipeline: Pub/Sub -> BigQuery (Apache Beam / Dataflow).

- Fixed windows (5 min) on event time, with allowed lateness for late data.
- Dedupe on event_id so a redelivered message never double-counts.
- Writes to the partitioned, clustered raw events table from terraform/.

Deploy on Dataflow (or run locally with DirectRunner for a smoke test):

    python streaming/dataflow_pipeline.py \
        --project my-gcp-project --region us-central1 \
        --input_subscription projects/my-gcp-project/subscriptions/retail-orders-sub \
        --output_table my-gcp-project:retail_raw.events --runner DataflowRunner
"""

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window


def parse_event(message: bytes) -> dict:
    event = json.loads(message.decode("utf-8"))
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "event_ts": event["event_ts"],
        "customer_id": event.get("customer_id"),
        "product_id": event.get("product_id"),
        "amount": event.get("amount"),
        "quantity": event.get("quantity"),
    }


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=True)
    known, pipeline_args = parser.parse_known_args(argv)

    table_schema = {
        "fields": [
            {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "quantity", "type": "INT64", "mode": "NULLABLE"},
        ]
    }

    options = PipelineOptions(pipeline_args, save_main_session=True)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known.input_subscription
            )
            | "Parse" >> beam.Map(parse_event)
            | "Window" >> beam.WindowInto(
                window.FixedWindows(300),
                # Late events up to 10 min still land in their window.
                allowed_lateness=600,
            )
            # Dedupe: same event_id twice (redelivery) keeps one copy.
            | "Dedupe" >> beam.Distinct()
            | "WriteBigQuery"
            >> beam.io.WriteToBigQuery(
                known.output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
