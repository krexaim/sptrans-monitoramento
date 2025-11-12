from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="transform_gold_fato",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,  
    catchup=False,
    tags=["sptrans", "transform", "gold", "fato"]
) as dag:
    
    task_fato_posicao = SparkSubmitOperator(
        task_id="transform_gold_fato_posicao",
        application="/opt/airflow/dags/utils/gold_fato_posicao.py",
        conn_id="spark_default",
        name="gold-fato-posicao",
        deploy_mode="client",
    )

    task_fato_posicao