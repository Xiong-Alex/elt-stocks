from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Daily reference-data refresh DAG.
#
# Purpose:
# - Pull lower-frequency company/reference datasets (fundamentals, dividends, earnings)
# - Materialize fact tables used by downstream analytics/feature views
with DAG(
    dag_id="company_fundamentals_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    # Fundamentals flow: ingest -> bronze -> silver -> gold -> fact.
    ingest_fundamentals = BashOperator(
        task_id="ingest_fundamentals_raw",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_fundamentals.py",
    )
    fundamentals_to_bronze = BashOperator(
        task_id="fundamentals_to_bronze",
        bash_command="python /opt/pipeline-jobs/bronze/spark_fundamentals_to_bronze.py",
    )
    fundamentals_to_silver = BashOperator(
        task_id="fundamentals_to_silver",
        bash_command="python /opt/pipeline-jobs/silver/spark_fundamentals_bronze_to_silver.py",
    )
    fundamentals_to_gold = BashOperator(
        task_id="fundamentals_to_gold",
        bash_command="python /opt/pipeline-jobs/gold/spark_fundamentals_silver_to_gold.py",
    )

    # Dividends flow: ingest -> bronze -> silver -> gold -> fact.
    ingest_dividends = BashOperator(
        task_id="ingest_dividends_raw",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_dividends.py",
    )
    dividends_to_bronze = BashOperator(
        task_id="dividends_to_bronze",
        bash_command="python /opt/pipeline-jobs/bronze/spark_dividends_to_bronze.py",
    )
    dividends_to_silver = BashOperator(
        task_id="dividends_to_silver",
        bash_command="python /opt/pipeline-jobs/silver/spark_dividends_bronze_to_silver.py",
    )
    dividends_to_gold = BashOperator(
        task_id="dividends_to_gold",
        bash_command="python /opt/pipeline-jobs/gold/spark_dividends_silver_to_gold.py",
    )

    # Earnings flow: ingest -> bronze -> silver -> gold -> fact.
    ingest_earnings = BashOperator(
        task_id="ingest_earnings_raw",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_earnings.py",
    )
    earnings_to_bronze = BashOperator(
        task_id="earnings_to_bronze",
        bash_command="python /opt/pipeline-jobs/bronze/spark_earnings_to_bronze.py",
    )
    earnings_to_silver = BashOperator(
        task_id="earnings_to_silver",
        bash_command="python /opt/pipeline-jobs/silver/spark_earnings_bronze_to_silver.py",
    )
    earnings_to_gold = BashOperator(
        task_id="earnings_to_gold",
        bash_command="python /opt/pipeline-jobs/gold/spark_earnings_silver_to_gold.py",
    )

    # Final serving-table fact builds.
    build_fact_fundamentals = BashOperator(
        task_id="build_fact_fundamentals",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_fundamentals.py",
    )

    # Step 2B: Build/refresh dividends fact from raw dividends source.
    build_fact_dividends = BashOperator(
        task_id="build_fact_dividends",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_dividends.py",
    )

    # Step 2C: Build/refresh earnings fact from raw earnings source.
    build_fact_earnings = BashOperator(
        task_id="build_fact_earnings",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_earnings.py",
    )

    # Per-domain dependency chains:
    # each domain completes staged checks before writing mart facts.
    ingest_fundamentals >> fundamentals_to_bronze >> fundamentals_to_silver >> fundamentals_to_gold >> build_fact_fundamentals
    ingest_dividends >> dividends_to_bronze >> dividends_to_silver >> dividends_to_gold >> build_fact_dividends
    ingest_earnings >> earnings_to_bronze >> earnings_to_silver >> earnings_to_gold >> build_fact_earnings
