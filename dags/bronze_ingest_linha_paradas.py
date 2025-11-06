from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.ingest_bronze import fetch_and_upload

with DAG(
    dag_id="ingest_to_bronze_linhas",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=["sptrans"]
) as dag:    

    task_linhas = PythonOperator(
        task_id="fetch_linhas",
        python_callable=lambda: fetch_and_upload("linhas")
    )

    task_paradas = PythonOperator(
        task_id="fetch_paradas",
        python_callable=lambda: fetch_and_upload("paradas")
    )

    task_linhas >> task_paradas
   