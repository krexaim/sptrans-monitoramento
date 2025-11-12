from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.ingest_bronze import fetch_and_upload


with DAG(
    dag_id="ingest_linhas_paradas",
    start_date=datetime(2025, 10, 10),
    schedule_interval="@daily",
    catchup=True,
    tags=["sptrans", "ingest", "transform", "linhas", "paradas"]
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
        conn_id="spark_default",
        name="sptrans-linhas-spark",
        deploy_mode="client",
    )

    task_transform_paradas = SparkSubmitOperator(
        task_id="transform_paradas_bronze_silver",
        application="/opt/airflow/dags/utils/transform_paradas_bronze_silver.py",
        conn_id="spark_default",
        name="sptrans-paradas-spark",
        deploy_mode="client",
    )

    # Trigger gold

    trigger_gold_dim = TriggerDagRunOperator(
        task_id="trigger_transform_gold_dim",
        trigger_dag_id="transform_gold_dimension",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Dependências (bronze → silver)
    [task_linhas, task_paradas] >> task_transform_linhas >> task_transform_paradas >> trigger_gold_dim
     