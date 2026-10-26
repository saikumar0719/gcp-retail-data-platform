"""Daily retail DAG (Airflow / Cloud Composer 3).

Runs the PySpark aggregate, then gates on data-quality checks before the
marts are marked ready. Retries + SLA + idempotent writes: a re-run of a
failed day overwrites that day's partition instead of duplicating it.

Deploy: copy to your Composer environment's dags/ folder.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from data_quality.checks import run_all_checks

default_args = {
    "owner": "retail-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="retail_daily",
    default_args=default_args,
    start_date=datetime(2026, 10, 26),
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "gcp"],
) as dag:

    aggregate = BashOperator(
        task_id="daily_sales_agg",
        bash_command=(
            "spark-submit batch/spark_jobs/daily_sales_agg.py "
            "--input gs://{{ var.value.project }}-retail-raw/events/ "
            "--output gs://{{ var.value.project }}-retail-curated/daily_sales/ "
            "--date {{ ds }}"
        ),
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=run_all_checks,
        op_kwargs={"date": "{{ ds }}"},
    )

    aggregate >> quality_gate
