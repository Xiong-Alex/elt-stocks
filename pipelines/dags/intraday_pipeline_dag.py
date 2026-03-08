from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Scheduled intraday price pipeline.
#
# Purpose:
# - Execute full price path on a recurring cadence:
#   ingest -> bronze -> silver -> gold.
# - Keep analytics tables updated with recent price movements.
with DAG(
    dag_id="intraday_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    # Step 1: Ingest recent price data to Kafka/source table in intraday mode.
    # - Uses templated run_id so all downstream tasks process same batch.
    ingest_prices = BashOperator(
        task_id="ingest_prices_to_kafka",
        bash_command=(
            "python /opt/pipeline-jobs/ingest/ingest_yfinance_to_kafka.py "
            "--mode intraday "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 2: Consume run batch from Kafka and persist Bronze rows.
    to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command=(
            "python /opt/pipeline-jobs/bronze/spark_kafka_to_bronze.py "
            "--mode intraday "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 3: Transform Bronze records into curated Silver records.
    to_silver = BashOperator(
        task_id="spark_to_silver",
        bash_command=(
            "python /opt/pipeline-jobs/silver/spark_bronze_to_silver.py "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 4: Merge Silver output into Gold serving table(s) in analytics DB.
    to_gold = BashOperator(
        task_id="spark_to_gold",
        bash_command=(
            "python /opt/pipeline-jobs/gold/spark_silver_to_gold.py "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Linear dependency chain keeps batch lineage clear and deterministic.
    ingest_prices >> to_bronze >> to_silver >> to_gold
