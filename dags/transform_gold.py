from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="transform_gold",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "transform", "gold", "dimension"]
) as dag:    
    
    task_gold= SparkSubmitOperator(
        task_id="transform_gold_dim_linha",
        application="/opt/airflow/dags/utils/gold_dim_linha.py",
        conn_id="spark_default",
        name="gold-dim-linha",
        verbose=True,
        deploy_mode="client",
    )

    # task_gold= SparkSubmitOperator(
    #     task_id="transform_gtfs_silver_gold",
    #     application="/opt/airflow/dags/utils/transform_gtfs_silver_gold.py",
    #     conn_id="spark_default",
    #     name="gtfs-silver-gold",
    #     verbose=True,
    #     deploy_mode="client",
    # )    

    task_gold