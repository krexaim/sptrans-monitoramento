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
import os
import glob
import json
import csv
import re
import argparse
from collections import defaultdict
from typing import Any, Dict, Set, List

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--fact', required=True, help='Arquivo JSON (fato)')
    p.add_argument('--dims', required=True, help='Pasta com arquivos dimensão ou lista separado por vírgula')
    p.add_argument('--out', default='summary_matches.csv', help='CSV de saída (parametro, correspondencia)')
    p.add_argument('--normalize', choices=['none','upper','digits','alnum'], default='upper',
                   help='Normalização aplicada (default=upper)')
    p.add_argument('--max-rows-per-dim', type=int, default=0, help='Limite de leitura por arquivo dimensão (0 = sem limite)')
    p.add_argument('--quotechar', default='"', help='Char usado para aspas em CSV')
    return p.parse_args()

def normalize_value(s: Any, mode: str) -> str:
    if s is None:
        return ''
    s = str(s).strip()
    if mode == 'none':
        return s
    if mode == 'upper':
        return s.upper()
    if mode == 'digits':
        return ''.join(re.findall(r'\d+', s))
    if mode == 'alnum':
        return ''.join(re.findall(r'[A-Za-z0-9]+', s)).upper()
    return s

def flatten_json(obj: Any, prefix: str = ''):
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_pref = f"{prefix}.{k}" if prefix else k
            results.extend(flatten_json(v, new_pref))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(flatten_json(item, prefix))
    else:
        results.append((prefix, obj))
    return results

def extract_fields_from_json(path: str, normalize_mode: str):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # heurística: sua estrutura tem 'l' com lista de registros
    if isinstance(data, dict) and 'l' in data and isinstance(data['l'], list):
        records = data['l']
    elif isinstance(data, list):
        records = data
    else:
        records = [data] if isinstance(data, dict) else []
    field_values = defaultdict(set)  # campo -> set of normalized values
    for rec in records:
        for path_key, val in flatten_json(rec):
            if val is None:
                continue
            norm = normalize_value(val, normalize_mode)
            if norm != '':
                field_values[path_key].add(norm)
    return field_values

def detect_delimiter(path: str, quotechar: str):
    try:
        sample = open(path, 'r', encoding='utf-8', errors='ignore').read(2048)
        # heurística simples
        for d in [',',';','\t','|']:
            if d in sample:
                return d
    except Exception:
        pass
    return ','

def read_all_values_from_file(path: str, normalize_mode: str, max_rows: int, quotechar: str) -> Set[str]:
    delim = detect_delimiter(path, quotechar)
    vals = set()
    with open(path, 'r', encoding='utf-8-sig', errors='ignore') as f:
        for i, line in enumerate(f):
            if max_rows and i >= max_rows:
                break
            # split pelo delimitador detectado (não usa csv module pra ser tolerante)
            parts = [p.strip().strip(quotechar) for p in line.split(delim)]
            for p in parts:
                if p == '':
                    continue
                vals.add(normalize_value(p, normalize_mode))
    return vals

def main():
    args = parse_args()

    # arquivos dimensão
    if os.path.isdir(args.dims):
        dim_files = sorted(glob.glob(os.path.join(args.dims, '*')))
    else:
        if ',' in args.dims:
            dim_files = [p.strip() for p in args.dims.split(',')]
        else:
            dim_files = [args.dims]

    # extrair campos do JSON
    field_values = extract_fields_from_json(args.fact, args.normalize)
    field_keys = sorted(field_values.keys())

    # ler valores de cada arquivo dimensão (agregados por arquivo)
    dim_values_by_file: Dict[str, Set[str]] = {}
    for df in dim_files:
        if not os.path.isfile(df):
            continue
        vals = read_all_values_from_file(df, args.normalize, args.max_rows_per_dim, args.quotechar)
        dim_values_by_file[os.path.basename(df)] = vals

    # construir mapeamento parametro -> lista de arquivos onde há pelo menos um match
    param_to_files: Dict[str, List[str]] = {}
    for fk in field_keys:
        matched_files = []
        json_vals = field_values[fk]
        for fname, dim_vals in dim_values_by_file.items():
            if not dim_vals:
                continue
            # se houver interseção non-empty
            if json_vals & dim_vals:
                matched_files.append(fname)
        param_to_files[fk] = matched_files

    # gravar CSV resumido
    out_path = args.out
    with open(out_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        writer.writerow(['parametro','correspondencia'])
        for fk in field_keys:
            files = param_to_files.get(fk, [])
            writer.writerow([fk, ';'.join(files)])

    print(f"Resumo gerado: {out_path}")
    print("Exemplo (primeiras linhas):")
    for fk in field_keys[:10]:
        print(f" - {fk} -> {param_to_files.get(fk, [])}")

if __name__ == '__main__':
    main()
