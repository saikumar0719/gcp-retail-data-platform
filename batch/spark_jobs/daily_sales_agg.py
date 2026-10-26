"""Daily sales aggregate (PySpark, Dataproc).

Reads the previous day's raw order events and writes one row per
product per day. Idempotent: the target partition is overwritten, so a
re-run never double-counts.

    spark-submit batch/spark_jobs/daily_sales_agg.py \
        --input gs://<project>-retail-raw/events/ \
        --output gs://<project>-retail-curated/daily_sales/ \
        --date 2026-10-26
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("daily-sales-agg").getOrCreate()

    events = spark.read.parquet(args.input)
    orders = events.filter(
        (F.col("event_type") == "order")
        & (F.to_date("event_ts") == F.lit(args.date))
    )

    daily = (
        orders.groupBy("product_id", F.to_date("event_ts").alias("sale_date"))
        .agg(
            F.count("*").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("amount").alias("revenue"),
            # Window function: rank products by revenue within the day.
            F.rank().over(
                Window.partitionBy(F.to_date("event_ts")).orderBy(
                    F.desc("revenue")
                )
            ).alias("revenue_rank"),
        )
        .repartition(1)
    )

    (
        daily.write.mode("overwrite")
        .partitionBy("sale_date")
        .parquet(args.output)
    )
    print(f"Wrote {daily.count()} daily-sales rows for {args.date}")
    spark.stop()


if __name__ == "__main__":
    main()
