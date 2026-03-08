from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Daily reference-data refresh DAG.
#
# Purpose:
# - Pull lower-frequency company/reference datasets (fundamentals, dividends, earnings)
# - Materialize fact tables used by downstream analytics/feature views
with DAG(
    dag_id="reference_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    # Step 1A: Ingest latest fundamentals snapshot into raw/source table.
    ingest_fundamentals = BashOperator(
        task_id="ingest_fundamentals_raw",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_fundamentals.py",
    )

    # Step 1B: Ingest dividends history/updates into raw/source table.
    ingest_dividends = BashOperator(
        task_id="ingest_dividends_raw",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_dividends.py",
    )

    # Step 1C: Ingest earnings history/updates into raw/source table.
    ingest_earnings = BashOperator(
        task_id="ingest_earnings_raw",
        bash_command="python /opt/pipeline-jobs/ingest/ingest_yfinance_earnings.py",
    )

    # Step 2A: Build/refresh fundamentals fact from raw fundamentals source.
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
    # each fact build waits only for its matching ingest step.
    ingest_fundamentals >> build_fact_fundamentals
    ingest_dividends >> build_fact_dividends
    ingest_earnings >> build_fact_earnings
