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

def fetch_data_posicao():
    endpoint = "/Posicao"  
    url = f"{SPTRANS_BASE_URL}{endpoint}"
    print(f"Fetching posição from {url}")

    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"❌ Failed to get posição: {response.status_code} - {response.text}")


def fetch_data_linhas():
    authenticate()
    linhas = []
    termos = list("123456789n")

    for termo in termos:
        url = f"{SPTRANS_BASE_URL}/Linha/Buscar?termosBusca={termo}"
        resp = session.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                linhas.extend(data)
    # remover duplicadas
    unique_linhas = {linha["cl"]: linha for linha in linhas}
    return list(unique_linhas.values())

# adicionar mais funcoes depois
#def get_bus_previsao():
#   url = f"{SPTRANS_BASE_URL}/Previsao/ParaParada?codigoLinha={{codigoLinha}}&parada={{codigoParada}}"