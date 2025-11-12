from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="transform_gold_dimension",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "transform", "gold", "dimension"]
) as dag:    
    
    # Silver -> Gold 
    task_gold_linha= SparkSubmitOperator(
        task_id="transform_gold_dim_linha",
        application="/opt/airflow/dags/utils/transform_gold_dim_linha.py",
        conn_id="spark_default",
        name="gold-dim-linha",
        deploy_mode="client",
    )

    task_gold_parada= SparkSubmitOperator(
        task_id="transform_gold_dim_parada",
        application="/opt/airflow/dags/utils/transform_gold_dim_parada.py",
        conn_id="spark_default",
        name="gold-dim-parada",
        deploy_mode="client",
    )

    task_gold_linha >> task_gold_parada