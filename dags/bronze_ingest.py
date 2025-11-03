from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

from utils.ingest_bronze import fetch_and_upload

with DAG(
    dag_id="ingest_to_bronze",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,#"*/2 * * * *",
    catchup=False,
    tags=["sptrans"]
) as dag:    

    task_posicao = PythonOperator(
        task_id="fetch_posicao",
        python_callable=lambda: fetch_and_upload("posicao")
    )

    task_transform = SparkSubmitOperator(
        task_id="transform_bronze_to_silver",
        application="/opt/airflow/dags/utils/transform_bronze_parquet.py",
        conn_id="spark_default",
        verbose=True,
    )    

    # exemplos
    # task_linhas = PythonOperator(
    #     task_id="fetch_linhas",
    #     python_callable=lambda: fetch_and_upload("linhas")
    # )

    # task_previsao = PythonOperator(
    #    task_id="fetch_previsao",
    #    python_callable=lambda: fetch_and_upload("previsao")
    # )
    # ordem
    #  task_posicao >> etc.