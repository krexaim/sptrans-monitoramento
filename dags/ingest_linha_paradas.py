from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.ingest_bronze import fetch_and_upload


with DAG(
    dag_id="ingest_to_bronze_linhas",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=["sptrans"]
) as dag:    
    # Bronze
    task_linhas = PythonOperator(
        task_id="fetch_linhas",
        python_callable=lambda: fetch_and_upload("linhas")
    )

    task_paradas = PythonOperator(
        task_id="fetch_paradas",
        python_callable=lambda: fetch_and_upload("paradas")
    )

    # Silver

    task_transform_linhas = SparkSubmitOperator(
        task_id="transform_linhas_bronze_silver",
        application="/opt/airflow/dags/utils/transform_linhas_bronze_silver.py",
        conn_id="spark_default",     # mesma conexão do seu Airflow
        name="sptrans-linhas-spark",
        verbose=True,
        deploy_mode="client",
    )

    task_transform_paradas = SparkSubmitOperator(
        task_id="transform_paradas_bronze_silver",
        application="/opt/airflow/dags/utils/transform_paradas_bronze_silver.py",
        conn_id="spark_default",
        name="sptrans-paradas-spark",
        verbose=True,
        deploy_mode="client",
    )

    # Dependências (bronze → silver)
    task_linhas >> task_paradas >> task_transform_linhas >> task_transform_paradas
     