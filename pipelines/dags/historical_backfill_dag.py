from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="historical_backfill_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    ingest_prices = BashOperator(
        task_id="ingest_prices_to_kafka",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_to_kafka.py --mode backfill",
    )

    spark_to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command="python /opt/pipeline-jobs/bronze/spark_kafka_to_bronze.py --mode backfill",
    )

    spark_to_silver = BashOperator(
        task_id="spark_to_silver",
        bash_command="python /opt/pipeline-jobs/silver/spark_bronze_to_silver.py",
    )

    spark_to_gold = BashOperator(
        task_id="spark_to_gold",
        bash_command="python /opt/pipeline-jobs/gold/spark_silver_to_gold.py",
    )

    ingest_prices >> spark_to_bronze >> spark_to_silver >> spark_to_gold

