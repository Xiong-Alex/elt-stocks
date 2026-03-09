from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Manual historical backfill DAG.
#
# Purpose:
# - Load historical price windows by date range/symbol scope.
# - Rebuild downstream layers for that run batch.
with DAG(
    dag_id="stock_historical_backfill_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # Step 1: Backfill ingest.
    # - Reads dag_run.conf params: start_date, end_date, symbols, run_id.
    # - Publishes to Kafka and writes source event rows for traceability.
    ingest_prices = BashOperator(
        task_id="ingest_prices_to_kafka",
        bash_command=(
            "python /opt/pipeline-jobs/ingest/ingest_yfinance_to_kafka.py "
            "--mode backfill "
            "--start-date '{{ dag_run.conf.get(\"start_date\", \"2020-01-01\") }}' "
            "--end-date '{{ dag_run.conf.get(\"end_date\", ds) }}' "
            "--symbols '{{ dag_run.conf.get(\"symbols\", \"\") }}' "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 2: Bronze load for this backfill run_id.
    spark_to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command=(
            "python /opt/pipeline-jobs/bronze/spark_kafka_to_bronze.py "
            "--mode backfill "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 3: Silver transform for this run batch.
    spark_to_silver = BashOperator(
        task_id="spark_to_silver",
        bash_command=(
            "python /opt/pipeline-jobs/silver/spark_bronze_to_silver.py "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 4: Gold merge for this run batch.
    spark_to_gold = BashOperator(
        task_id="spark_to_gold",
        bash_command=(
            "python /opt/pipeline-jobs/gold/spark_silver_to_gold.py "
            "--run-id '{{ dag_run.conf.get(\"run_id\", ts_nodash) }}'"
        ),
    )

    # Step 5: Refresh price daily fact after Gold is updated.
    build_fact_price_daily = BashOperator(
        task_id="build_fact_price_daily",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_price_daily.py",
    )

    # Full ordered backfill chain.
    ingest_prices >> spark_to_bronze >> spark_to_silver >> spark_to_gold >> build_fact_price_daily
