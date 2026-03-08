from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Manual price-to-bronze DAG.
#
# Purpose:
# - Run a focused price ingestion cycle without Silver/Gold transforms.
# - Useful for manual Bronze refreshes and debugging ingest/bronze path.
with DAG(
    dag_id="price_bronze_streaming_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    # Step 1: Pull price data and publish events to Kafka.
    # - Uses intraday mode windowing in ingest job.
    # - run_id is templated so downstream Bronze step filters the same batch.
    ingest_prices = BashOperator(
        task_id="ingest_prices_to_kafka",
        bash_command=(
            "python /opt/pipeline-jobs/ingest/ingest_yfinance_to_kafka.py "
            "--mode intraday "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 2: Consume run_id-scoped events from Kafka and write Bronze tables.
    # - Valid rows -> public.stock_bars_bronze
    # - Invalid rows -> public.stock_bars_quarantine
    to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command=(
            "python /opt/pipeline-jobs/bronze/spark_kafka_to_bronze.py "
            "--mode intraday "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Strict dependency: ingest must complete before Bronze consumption.
    ingest_prices >> to_bronze
