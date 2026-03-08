from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# Daily stock-universe refresh DAG.
#
# Purpose:
# - Refresh symbol/universe source membership state.
# - Rebuild universe-related dimensions and bridge mappings.
with DAG(
    dag_id="update_stock_universe_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    # Step 1: Refresh source symbols + universe memberships (default source: sp500).
    refresh_symbols = BashOperator(
        task_id="refresh_stock_symbols",
        bash_command="python /opt/pipeline-jobs/marts/refresh_stock_symbols.py --source sp500",
    )

    # Step 2: Build stock dimension from refreshed symbol source.
    update_dim_stock = BashOperator(
        task_id="build_dim_stock",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_stock.py",
    )

    # Step 3: Build universe dimension from refreshed membership source.
    update_dim_universe = BashOperator(
        task_id="build_dim_universe",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_universe.py",
    )

    # Step 4: Build stock-universe bridge table from dimensions + memberships.
    update_bridge_stock_universe = BashOperator(
        task_id="build_bridge_stock_universe_membership",
        bash_command="python /opt/pipeline-jobs/marts/build_bridge_stock_universe_membership.py",
    )

    # Ordered dependency chain keeps universe lineage deterministic.
    refresh_symbols >> update_dim_stock >> update_dim_universe >> update_bridge_stock_universe
