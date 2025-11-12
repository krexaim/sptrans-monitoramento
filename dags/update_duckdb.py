from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="update_duckdb",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,  
    catchup=False,
    tags=["duckdb", "metabase", "gold"]
) as dag:

    task_build_duckdb = BashOperator(
        task_id="build_duckdb",
        bash_command="python /opt/airflow/dags/utils/build_duckdb.py"
    )