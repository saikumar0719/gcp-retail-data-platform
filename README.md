# GCP Retail Data Platform

An end-to-end senior data engineering project: a simulated e-commerce store
streaming orders, clicks, and inventory events through Kafka into Google Cloud,
landing in BigQuery, with batch aggregates orchestrated by Airflow/Cloud
Composer, data-quality gates, and a MongoDB product catalog.

Built to demonstrate, hands-on: streaming + batch pipelines, BigQuery SQL,
PySpark, Airflow orchestration, Kafka, MongoDB, Terraform, Docker, and CI.

## Architecture

```
Simulated store events
        │  (Kafka producer, local Docker)
        ▼
   Kafka topics ──► Kafka → Pub/Sub bridge ──► Pub/Sub
                                                     │
                                                     ▼
                                          Dataflow (Beam, streaming)
                                                     │  window + dedupe
                                                     ▼
                                    BigQuery: raw events (partitioned)
                                                     │
                        ┌────────────────────────────┘
                        ▼
        Composer/Airflow DAG ──► PySpark (Dataproc) ──► BigQuery marts
                        │                                   │
                        ▼                                   ▼
              Data-quality checks                    Dashboards
              (counts, dupes, nulls,                      (Looker Studio)
               reconciliation, alerts)
                        │
   MongoDB ◄── product catalog loader
```

## Repository layout

| Path | What it is |
| --- | --- |
| `producers/` | Kafka event producer (orders, clicks, inventory) + local Docker stack |
| `streaming/` | Kafka → Pub/Sub bridge, Beam/Dataflow streaming pipeline |
| `batch/` | PySpark aggregate jobs + Airflow DAGs |
| `sql/` | BigQuery staging/mart models (CTEs, window functions) + sample queries |
| `data_quality/` | Reusable quality checks + reconciliation |
| `mongodb/` | Product catalog loader |
| `terraform/` | GCP infra: buckets, Pub/Sub, BigQuery datasets (Composer optional) |
| `dashboards/` | Dashboard spec |
| `tests/` | Unit tests for the quality checks |

## Run it (local first, zero GCP cost)

```bash
# 1. Start Kafka + MongoDB locally
docker compose -f producers/docker-compose.yml up -d

# 2. Generate events into Kafka
pip install -r producers/requirements.txt
python producers/kafka_producer.py --events 1000

# 3. Forward Kafka -> Pub/Sub (needs GCP creds; skip locally, see streaming/)
python streaming/kafka_to_pubsub.py --dry-run

# 4. Run the quality checks + unit tests
pip install -r data_quality/requirements.txt
pytest tests/ -q
```

## Run it on GCP (small scale, shut down after)

```bash
cd terraform
terraform init && terraform apply   # buckets, Pub/Sub, BigQuery datasets
# Deploy the Dataflow job, then the Composer DAG in batch/dags/
# Keep event volumes small and delete the Composer environment when done —
# Composer is the only expensive piece here.
```

## What this demonstrates (recruiter talking points)

- **Streaming:** Kafka producer → bridge → Pub/Sub → Dataflow → BigQuery,
  with event-time windowing, watermarking, and dedupe on `event_id`.
- **BigQuery SQL:** partitioned/clustered tables, CTEs, window functions
  (running totals, ranking), cost-aware queries (no `SELECT *`).
- **Batch:** PySpark aggregates on Dataproc, orchestrated by an Airflow DAG
  with retries, SLAs, and idempotent loads.
- **Data quality:** source-to-target reconciliation, duplicate/null checks,
  alert on mismatch — the same checks used in healthcare/financial reporting.
- **NoSQL:** MongoDB product catalog with embedding-vs-referencing schema
  decisions documented in `mongodb/`.
- **Infra as code:** Terraform for all GCP resources; Docker for local Kafka
  and MongoDB; GitHub Actions CI runs the tests.

## Cost notes

Local Docker + unit tests cost nothing. On GCP, Pub/Sub, Dataflow, and
BigQuery with small simulated volumes stay in free-tier/low-cost territory.
Cloud Composer is the expensive part — create it only when testing the DAG,
delete it after.
