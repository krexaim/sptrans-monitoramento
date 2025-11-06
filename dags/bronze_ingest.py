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
        name="arrow-spark",
        verbose=True,
        deploy_mode="client",
        conf={
            "spark.master": "local[*]"
        },
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    )

    task_posicao >> task_transform