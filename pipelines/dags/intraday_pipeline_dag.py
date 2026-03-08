from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="intraday_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    ingest_prices = BashOperator(
        task_id="ingest_prices_to_kafka",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_to_kafka.py --mode intraday",
    )

    ingest_fundamentals = BashOperator(
        task_id="ingest_fundamentals",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_fundamentals.py",
    )

    ingest_dividends = BashOperator(
        task_id="ingest_dividends",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_dividends.py",
    )

    ingest_earnings = BashOperator(
        task_id="ingest_earnings",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_earnings.py",
    )

    to_silver = BashOperator(
        task_id="spark_to_silver",
        bash_command="python /opt/pipeline-jobs/silver/spark_bronze_to_silver.py",
    )

    to_gold = BashOperator(
        task_id="spark_to_gold",
        bash_command="python /opt/pipeline-jobs/gold/spark_silver_to_gold.py",
    )

    [ingest_prices, ingest_fundamentals, ingest_dividends, ingest_earnings] >> to_silver >> to_gold

