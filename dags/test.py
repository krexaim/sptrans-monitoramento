from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import os
import requests
import json
import io
from minio import Minio
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/opt/airflow/.env")

SPTRANS_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
SPTRANS_API_KEY = os.getenv("SPTRANS_API_KEY")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "bronze"

session = requests.Session()

def authenticate():
    url = f"{SPTRANS_BASE_URL}/Login/Autenticar?token={SPTRANS_API_KEY}"
    response = session.post(url)
    if response.status_code == 200 and response.json() is True:
        print("✅ Authenticated with SPTrans API.")
    else:
        raise Exception(f"❌ Authentication failed: {response.status_code} - {response.text}")

def get_bus_positions():
    url = f"{SPTRANS_BASE_URL}/Posicao"
    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"❌ Failed to get positions: {response.status_code} - {response.text}")

def upload_to_minio(data):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"bus_positions_{timestamp}.json"

    data_bytes = io.BytesIO(json.dumps(data).encode("utf-8"))

    client.put_object(
        MINIO_BUCKET,
        file_name,
        data_bytes,
        length=len(data_bytes.getvalue()),
        content_type="application/json"
    )

    print(f"✅ Uploaded {file_name} to MinIO/{MINIO_BUCKET}")

def fetch_and_upload_bus_positions():
    authenticate()
    data = get_bus_positions()
    upload_to_minio(data)

# Define the DAG
with DAG(
    dag_id="sptrans_bus_positions",
    start_date=datetime(2025, 10, 10),
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    tags=["sptrans"]
) as dag:
    task_fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload_bus_positions",
        python_callable=fetch_and_upload_bus_positions
    )
