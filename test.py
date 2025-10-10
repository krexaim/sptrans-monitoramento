import os
import requests
import json
import io
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SPTRANS_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
SPTRANS_API_KEY = os.getenv("SPTRANS_API_KEY")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "bronze"

session = requests.Session()

auth_url = f"{SPTRANS_BASE_URL}/Login/Autenticar?token={SPTRANS_API_KEY}"
auth_response = session.post(auth_url)

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

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
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

def main():
    try:
        authenticate()
        data = get_bus_positions()
        upload_to_minio(data)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()   
