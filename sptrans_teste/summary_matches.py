#!/usr/bin/env python3
"""
summary_matches_simple.py

Gera um CSV resumido com duas colunas:
  parametro, correspondencia

parametro = nome do campo do JSON (ex: c, cl, lt0, vs.p, ...)
correspondencia = lista de arquivos txt onde pelo menos um valor do campo foi encontrado (arquivo1;arquivo2)

Uso:
  python3 summary_matches_simple.py --fact posicoes.json --dims ./dims/ --out summary_matches.csv

Opções:
  --normalize [none|upper|digits|alnum]   (default: upper)
  --max-rows-per-dim N                   (0 = sem limite)
  --quotechar CHAR                       (padrão: ")
"""
import os                # operações com sistema de arquivos
import glob              # busca de arquivos com curingas (*)
import json              # leitura e manipulação de arquivos JSON
import csv               # escrita de arquivos CSV
import re                # expressões regulares (usado na normalização)
import argparse          # leitura de argumentos de linha de comando
from collections import defaultdict   # dicionário com valor padrão
from typing import Any, Dict, Set, List  # tipos para anotações

# --------------------------------------------------------
# Função que configura e lê os argumentos passados ao script
# --------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--fact', required=True, help='Arquivo JSON (fato)')
    p.add_argument('--dims', required=True, help='Pasta com arquivos dimensão ou lista separado por vírgula')
    p.add_argument('--out', default='summary_matches.csv', help='CSV de saída (parametro, correspondencia)')
    p.add_argument('--normalize', choices=['none','upper','digits','alnum'], default='upper',
                   help='Normalização aplicada (default=upper)')
    p.add_argument('--max-rows-per-dim', type=int, default=0, help='Limite de leitura por arquivo dimensão (0 = sem limite)')
    p.add_argument('--quotechar', default='"', help='Char usado para aspas em CSV')
    return p.parse_args()   # retorna objeto com os argumentos

# --------------------------------------------------------
# Função que aplica diferentes tipos de normalização a um valor
# --------------------------------------------------------
def normalize_value(s: Any, mode: str) -> str:
    if s is None:
        return ''           # retorna string vazia se valor for None
    s = str(s).strip()      # converte para string e remove espaços extras
    if mode == 'none':
        return s            # não faz nada
    if mode == 'upper':
        return s.upper()    # converte tudo para maiúsculas
    if mode == 'digits':
        return ''.join(re.findall(r'\d+', s))  # mantém apenas dígitos
    if mode == 'alnum':
        # mantém apenas letras e números e converte para maiúsculo
        return ''.join(re.findall(r'[A-Za-z0-9]+', s)).upper()
    return s                # fallback

# --------------------------------------------------------
# Função recursiva que "achata" (flatten) um JSON
# para gerar pares (chave, valor) com caminho completo
# Exemplo: {'a': {'b': 1}} -> [('a.b', 1)]
# --------------------------------------------------------
def flatten_json(obj: Any, prefix: str = ''):
    results = []
    if isinstance(obj, dict):              # se for dicionário
        for k, v in obj.items():           # percorre chaves e valores
            new_pref = f"{prefix}.{k}" if prefix else k   # adiciona prefixo
            results.extend(flatten_json(v, new_pref))     # chama recursivamente
    elif isinstance(obj, list):            # se for lista
        for item in obj:                   # percorre cada item
            results.extend(flatten_json(item, prefix))    # mantém mesmo prefixo
    else:
        results.append((prefix, obj))      # valor simples: salva o par (campo, valor)
    return results

# --------------------------------------------------------
# Extrai todos os campos e valores de um JSON (arquivo de fato)
# Retorna: dicionário {campo: conjunto de valores normalizados}
# --------------------------------------------------------
def extract_fields_from_json(path: str, normalize_mode: str):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)               # lê e carrega o JSON completo

    # Detecta formato: alguns arquivos têm chave 'l' contendo lista
    if isinstance(data, dict) and 'l' in data and isinstance(data['l'], list):
        records = data['l']
    elif isinstance(data, list):
        records = data                    # JSON é uma lista
    else:
        records = [data] if isinstance(data, dict) else []  # único objeto

    field_values = defaultdict(set)        # dicionário campo -> conjunto de valores

    for rec in records:                    # percorre cada registro do JSON
        for path_key, val in flatten_json(rec):  # gera pares (campo, valor)
            if val is None:
                continue
            norm = normalize_value(val, normalize_mode)  # normaliza o valor
            if norm != '':
                field_values[path_key].add(norm)          # adiciona ao conjunto
    return field_values

# --------------------------------------------------------
# Detecta delimitador provável (vírgula, ponto e vírgula, tab, pipe)
# --------------------------------------------------------
def detect_delimiter(path: str, quotechar: str):
    try:
        sample = open(path, 'r', encoding='utf-8', errors='ignore').read(2048)
        # verifica qual delimitador aparece no trecho lido
        for d in [',',';','\t','|']:
            if d in sample:
                return d
    except Exception:
        pass
    return ','   # padrão caso não detecte nenhum

# --------------------------------------------------------
# Lê todos os valores de um arquivo dimensão (TXT/CSV)
# Retorna um conjunto de valores normalizados
# --------------------------------------------------------
def read_all_values_from_file(path: str, normalize_mode: str, max_rows: int, quotechar: str) -> Set[str]:
    delim = detect_delimiter(path, quotechar)   # tenta descobrir delimitador
    vals = set()
    with open(path, 'r', encoding='utf-8-sig', errors='ignore') as f:
        for i, line in enumerate(f):
            if max_rows and i >= max_rows:      # respeita limite de linhas
                break
            # divide a linha pelo delimitador detectado
            parts = [p.strip().strip(quotechar) for p in line.split(delim)]
            for p in parts:
                if p == '':
                    continue
                # normaliza cada valor e adiciona ao conjunto
                vals.add(normalize_value(p, normalize_mode))
    return vals

# --------------------------------------------------------
# Função principal: orquestra o processo completo
# --------------------------------------------------------
def main():
    args = parse_args()   # lê argumentos da linha de comando

    # determina quais arquivos de dimensão usar
    if os.path.isdir(args.dims):
        # se for pasta, pega todos os arquivos dentro
        dim_files = sorted(glob.glob(os.path.join(args.dims, '*')))
    else:
        # se for lista separada por vírgulas
        if ',' in args.dims:
            dim_files = [p.strip() for p in args.dims.split(',')]
        else:
            dim_files = [args.dims]  # apenas um arquivo

    # extrai todos os campos e valores do JSON principal
    field_values = extract_fields_from_json(args.fact, args.normalize)
    field_keys = sorted(field_values.keys())  # ordena nomes dos campos

    # dicionário: arquivo dimensão -> conjunto de valores encontrados
    dim_values_by_file: Dict[str, Set[str]] = {}
    for df in dim_files:
        if not os.path.isfile(df):         # ignora se não for arquivo
            continue
        # lê e normaliza valores de cada arquivo dimensão
        vals = read_all_values_from_file(df, args.normalize, args.max_rows_per_dim, args.quotechar)
        dim_values_by_file[os.path.basename(df)] = vals  # guarda pelo nome do arquivo

    # cria mapeamento campo -> lista de arquivos onde há correspondência
    param_to_files: Dict[str, List[str]] = {}
    for fk in field_keys:
        matched_files = []
        json_vals = field_values[fk]      # conjunto de valores do campo do JSON
        for fname, dim_vals in dim_values_by_file.items():
            if not dim_vals:
                continue
            # verifica se há interseção entre valores do JSON e do arquivo
            if json_vals & dim_vals:
                matched_files.append(fname)
        param_to_files[fk] = matched_files  # guarda lista de arquivos correspondentes

    # grava o resultado no CSV de saída
    out_path = args.out
    with open(out_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        writer.writerow(['parametro','correspondencia'])  # cabeçalho
        for fk in field_keys:
            files = param_to_files.get(fk, [])
            # escreve linha: campo ; arquivos correspondentes separados por ponto e vírgula
            writer.writerow([fk, ';'.join(files)])

    # exibe resumo no terminal
    print(f"Resumo gerado: {out_path}")
    print("Exemplo (primeiras linhas):")
    for fk in field_keys[:10]:   # mostra primeiros 10 campos
        print(f" - {fk} -> {param_to_files.get(fk, [])}")

# --------------------------------------------------------
# Executa main() se o script for chamado diretamente
# --------------------------------------------------------
if __name__ == '__main__':
    main()
