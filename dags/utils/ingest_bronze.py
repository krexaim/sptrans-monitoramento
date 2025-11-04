from .sptrans_api import authenticate, fetch_data_posicao, fetch_data_linhas
from .minio_utils import upload_to_minio

FETCH_FUNCS = {
    "posicao": fetch_data_posicao,
    "linhas": fetch_data_linhas,
}

def fetch_and_upload(dataset_name):
    try:
        authenticate()
        if dataset_name not in FETCH_FUNCS:
            raise ValueError(f"Dataset '{dataset_name}' não suportado.")
        data = FETCH_FUNCS[dataset_name]()
        upload_to_minio(data, dataset_name)
    except Exception as e:
        raise Exception(f"❌ Erro ao processar dataset '{dataset_name}': {e}")
    
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pegar dados do SPTrans API e faz upload pro MinIO.")
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="Nome do dataset (ex: posicao, linhas, paradas)"
    )
    args = parser.parse_args()

    try:
        fetch_and_upload(args.dataset)
    except Exception as e:
        print(f"❌ Error: {e}")