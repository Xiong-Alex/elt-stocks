from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="price_bronze_streaming_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    run_price_stream = BashOperator(
        task_id="run_price_stream",
        bash_command="python /opt/pipeline-jobs/bronze/spark_kafka_to_bronze.py --mode streaming",
    )

