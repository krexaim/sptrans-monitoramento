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
    termos = list("123456789n")  # cobre todas as linhas

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
    # fetch_data_linhas() já autentica
    linhas = fetch_data_linhas()  # -> lista de dicts com "cl"
    cls = [item["cl"] for item in linhas]
    resultados = {}
    for cl in cls:
        url = f"{SPTRANS_BASE_URL}/Parada/BuscarParadasPorLinha?codigoLinha={cl}"
        try: 
            resp = session.get(url)
            resp.status_code == 200
            resultados[cl] = resp.json()
        except Exception as e:
            print(f"⚠️ Erro ao buscar paradas da linha {cl}: {resp.status_code}")
    print(f"✅ Total de paradas únicas coletadas: {len(cls)}")
    return resultados
