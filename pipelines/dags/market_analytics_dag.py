from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Feature/mart refresh DAG.
#
# Purpose:
# - Build analytical dimensions and facts used by dashboards/features.
# - Produce market signal features derived from price fact.
with DAG(
    dag_id="market_analytics_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    # Step 1: Build/extend date dimension from available price timeline.
    build_dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_date.py",
    )

    # Step 2: Build stock dimension (ticker-level dimensional table).
    build_dim_stock = BashOperator(
        task_id="build_dim_stock",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_stock.py",
    )

    # Step 3A: Build daily price fact from Gold bars.
    build_fact_price = BashOperator(
        task_id="build_fact_price_daily",
        bash_command="python /opt/pipeline-jobs/marts/build_fact_price_daily.py",
    )

    # Step 4: Build market signal features derived from price fact.
    build_market_signals = BashOperator(
        task_id="build_market_signals",
        bash_command="python /opt/pipeline-jobs/marts/build_market_signals.py",
    )

    # Dependencies:
    # - Build dimensions first.
    # - Build price fact for feature generation.
    # - Reference-data facts are owned by `company_fundamentals_dag`.
    # - Signals depend on price fact completion.
    build_dim_date >> build_dim_stock
    build_dim_stock >> build_fact_price
    build_fact_price >> build_market_signals
