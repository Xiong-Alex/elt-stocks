from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="feature_engineering_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    build_dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_date.py",
    )

    build_dim_stock = BashOperator(
        task_id="build_dim_stock",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_stock.py",
    )

    build_fact_price = BashOperator(
        task_id="build_fact_price_daily",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_price_daily.py",
    )

    build_fact_fundamentals = BashOperator(
        task_id="build_fact_fundamentals",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_fundamentals.py",
    )

    build_fact_dividends = BashOperator(
        task_id="build_fact_dividends",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_dividends.py",
    )

    build_fact_earnings = BashOperator(
        task_id="build_fact_earnings",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_earnings.py",
    )

    build_market_signals = BashOperator(
        task_id="build_market_signals",
        bash_command="python /opt/pipeline-jobs/marts/build_market_signals.py",
    )

    (
        build_dim_date
        >> build_dim_stock
        >> build_fact_price
        >> build_fact_fundamentals
        >> build_fact_dividends
        >> build_fact_earnings
        >> build_market_signals
    )

