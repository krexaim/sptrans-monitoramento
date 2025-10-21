import io
import json
from datetime import datetime, timezone
from minio import Minio
from .config import MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_ENDPOINT

MINIO_BUCKET = "bronze"

# Faz um upload pro minio com identificadores do dataset e timestamp
def upload_to_minio(data, dataset_name):  
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False  
    )

    now = datetime.now(timezone.utc)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    object_name = f"{dataset_name}/{year}/{month}/{day}/{dataset_name}_{timestamp}.json"
    data_bytes = io.BytesIO(json.dumps(data).encode("utf-8"))

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=data_bytes,
        length=len(data_bytes.getvalue()),
        content_type="application/json"
    )

    print(f"✅ Uploaded {object_name} to MinIO/{MINIO_BUCKET}")
