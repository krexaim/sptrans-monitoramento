import requests
from .config import SPTRANS_API_KEY

SPTRANS_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
session = requests.Session()

#------------------
# ENDPOINTS
#------------------
ENDPOINTS = { # {dataset : endpoint}
    "posicao": "/Posicao",
    "linhas": "/Linha/Buscar?termosBusca=a",     
    "paradas": "/Parada/Buscar?termosBusca=a",
}

def authenticate():
    url = f"{SPTRANS_BASE_URL}/Login/Autenticar?token={SPTRANS_API_KEY}"
    response = session.post(url)
    if response.status_code == 200 and response.json() is True:
        print("✅ Authenticated with SPTrans API.")
    else:
        raise Exception(f"❌ Authentication failed: {response.status_code} - {response.text}")

def fetch_data(dataset_name):
    if dataset_name not in ENDPOINTS:
        raise ValueError(f"Dataset '{dataset_name}' not configured.")

    endpoint = ENDPOINTS[dataset_name]    
    url = f"{SPTRANS_BASE_URL}{endpoint}"
    print(f"Fetching {dataset_name} from {url}")

    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"❌ Failed to get {dataset_name}: {response.status_code} - {response.text}")

def fetch_data_linhas():
    linhas = []
    with open ("bus_route_ids.txt", 'r') as arquivo:
     for linha in arquivo:
            linhas.append(linha.strip())

    endpoint = "/Linha/Buscar?termosBusca={linhas}"   
    url = f"{SPTRANS_BASE_URL}{endpoint}"
    print(f"Fetching linhas from {url}")

    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"❌ Failed to get linhas: {response.status_code} - {response.text}")
    
# adicionar mais funcoes depois
#def get_bus_previsao():
#   url = f"{SPTRANS_BASE_URL}/Previsao/ParaParada?codigoLinha={{codigoLinha}}&parada={{codigoParada}}"