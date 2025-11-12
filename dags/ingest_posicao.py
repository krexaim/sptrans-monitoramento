from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

from utils.ingest_bronze import fetch_and_upload

with DAG(
    dag_id="ingest_transform_posicao",
    start_date=datetime(2025, 10, 10),
    schedule_interval="*/2 * * * *",
    catchup=False,
    tags=["sptrans", "ingest", "transform", "posicao"]
) as dag:    

    task_posicao = PythonOperator(
        task_id="fetch_posicao",
        python_callable=lambda: fetch_and_upload("posicao")
    )

    task_transform = SparkSubmitOperator(
        task_id="transform_posicao_bronze_silver",
        application="/opt/airflow/dags/utils/transform_posicao_bronze_silver.py",
        conn_id="spark_default",
        name="posicao-bronze-silver",
        deploy_mode="client",
    )

    task_posicao >> task_transform