from .sptrans_api import authenticate, fetch_data, ENDPOINTS, SPTRANS_BASE_URL, session
from .minio_utils import upload_to_minio

def fetch_and_upload(dataset_name):
    try:
        authenticate()
        data = fetch_data(dataset_name)
        upload_to_minio(data, dataset_name)
    except Exception as e:
        raise Exception(f"❌ Erro ao processar dataset '{dataset_name}': {e}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pegar dados do SPTrans API e faz upload pro MinIO.")
    parser.add_argument("--dataset", type=str, required=True, help="Nome do dataset (ex: posicao, linhas, paradas)")
    args = parser.parse_args()

    try:
        fetch_and_upload(args.dataset)
    except Exception as e:
        print(f"❌ Error: {e}")