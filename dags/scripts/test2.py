from scripts.sptrans_api import authenticate, get_bus_positions
from scripts.minio_utils import upload_to_minio

def run():
    try:
        authenticate()
        data = get_bus_positions()
        upload_to_minio(data)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run()   
