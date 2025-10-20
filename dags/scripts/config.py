import os
from dotenv import load_dotenv

load_dotenv()

# Get environment variables helper
def get_env_var(key, default=None):
    return os.getenv(key, default)

# Config variables
SPTRANS_API_KEY = get_env_var("SPTRANS_API_KEY")
MINIO_ROOT_USER = get_env_var("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = (
    get_env_var("MINIO_ENDPOINT_DOCKER") if os.getenv("AIRFLOW_ENV") == "docker"
    else get_env_var("MINIO_ENDPOINT_LOCAL", "localhost:9000")
)