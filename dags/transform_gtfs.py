from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="transform_gtfs",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "transform"]
) as dag:    
    
    task_silver= SparkSubmitOperator(
        task_id="transform_gtfs_bronze_silver",
        application="/opt/airflow/dags/utils/transform_gtfs_bronze_silver.py",
        conn_id="spark_default",
        name="arrow-spark",
        verbose=True,
        deploy_mode="client",
    )

    task_silver #>> task_gold