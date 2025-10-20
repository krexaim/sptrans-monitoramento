from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import functions from modularized scripts
import scripts.test2

# Define the DAG
with DAG(
    dag_id="sptrans_bus_positions_test",
    start_date=datetime(2025, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=["sptrans"]
) as dag:    
    task_fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload_bus_positions",
        python_callable=scripts.test2.run
    )
