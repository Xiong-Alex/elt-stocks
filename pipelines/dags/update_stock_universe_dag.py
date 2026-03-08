from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="update_stock_universe_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    update_symbols = BashOperator(
        task_id="update_symbols",
        bash_command="python /opt/pipeline-jobs/marts/build_dim_stock.py",
    )

