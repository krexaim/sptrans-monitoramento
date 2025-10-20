from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import scripts.test

with DAG(
    dag_id="sptrans_bus_positions",
    start_date=datetime(2025, 10, 10),
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    tags=["sptrans"]
) as dag:
    task_fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload_bus_positions",
        python_callable=scripts.test.main
    )