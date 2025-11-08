import requests
from .config import SPTRANS_API_KEY

SPTRANS_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
session = requests.Session()

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
    linhas = {}
    termos = list("123456789n")  # letras e dígitos cobrem praticamente todas as linhas

    for termo in termos:
        url = f"{SPTRANS_BASE_URL}/Linha/Buscar?termosBusca={termo}"
        try:
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            for linha in resp.json():
                # Usa o código da linha como chave (único)
                linhas[linha["cl"]] = linha
        except Exception as e:
            print(f"⚠️ Erro ao buscar termo '{termo}': {e}")

    print(f"✅ Total de linhas únicas coletadas: {len(linhas)}")
    return list(linhas.values())

def fetch_data_paradas():
    return True


# adicionar mais funcoes depois
#def get_bus_previsao():
#   url = f"{SPTRANS_BASE_URL}/Previsao/ParaParada?codigoLinha={{codigoLinha}}&parada={{codigoParada}}"