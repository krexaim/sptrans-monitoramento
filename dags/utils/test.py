import json
from datetime import datetime
from .sptrans_api import authenticate, session, SPTRANS_BASE_URL

def get_all_linhas():
    authenticate()
    linhas = []
    termos = list("0123456789abcdefghijklmnopqrstuvwxyz")

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

def salvar_em_json(dados, nome_arquivo):
    """Salva uma lista ou dict em arquivo JSON local"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{nome_arquivo}_{timestamp}.json"

    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Dados salvos em: {file_name}")

if __name__ == "__main__":
    linhas = get_all_linhas()
    salvar_em_json(linhas, "linhas_ref")
import json
from datetime import datetime
from sptrans_api import authenticate, session, SPTRANS_BASE_URL

def get_all_linhas():
    authenticate()
    linhas = []
    termos = list("0123456789abcdefghijklmnopqrstuvwxyz")

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

def salvar_em_json(dados, nome_arquivo):
    """Salva uma lista ou dict em arquivo JSON local"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{nome_arquivo}_{timestamp}.json"

    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Dados salvos em: {file_name}")

if __name__ == "__main__":
    linhas = get_all_linhas()
    salvar_em_json(linhas, "linhas_ref")
