#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filtra apenas as rotas de ÔNIBUS a partir de GTFS/routes.txt e gera uma lista de route_id.

Uso (a partir da raiz do repo):
  python -m dags.utils.filter_bus_routes --routes GTFS/routes.txt --out_ids bus_route_ids.txt

Saída:
  - bus_route_ids.txt  (um route_id por linha, somente ônibus)

Regras GTFS (padrão):
  route_type == 1 -> Subway/Metro (REMOVER)
  route_type == 3 -> Bus (MANTER)
"""
import argparse
import csv
import os
import sys

# garantir import absoluto se precisar usar em DAGs (opcional; inofensivo)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

BUS_TYPE = "3"    # Bus
METRO_TYPE = "1"  # Subway/Metro

def read_routes(routes_path):
    with open(routes_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("routes.txt vazio ou sem cabeçalho.")
        required = {"route_type", "route_id"}
        missing = required - set(reader.fieldnames)
        if missing:
            raise ValueError(f"routes.txt precisa conter as colunas: {', '.join(sorted(required))}. Faltando: {', '.join(sorted(missing))}")
        rows = list(reader)
    return rows

def filter_buses(rows):
    """
    Mantém apenas linhas com route_type == 3 (ônibus) e remove route_type == 1 (metrô).
    Qualquer outro route_type é descartado por padrão (ajuste se necessário).
    """
    buses = []
    total_by_type = {}
    for r in rows:
        rt = (r.get("route_type") or "").strip()
        total_by_type[rt] = total_by_type.get(rt, 0) + 1
        if rt == BUS_TYPE:
            buses.append(r)
    return buses, total_by_type

def write_route_ids(bus_rows, out_ids_path):
    os.makedirs(os.path.dirname(out_ids_path) or ".", exist_ok=True)
    count = 0
    with open(out_ids_path, "w", encoding="utf-8") as out:
        for r in bus_rows:
            rid = (r.get("route_id") or "").strip()
            if rid:
                out.write(rid + "\n")
                count += 1
    return count

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--routes", required=True, help="Caminho para GTFS/routes.txt")
    ap.add_argument("--out_ids", default="bus_route_ids.txt", help="Arquivo de saída com route_id (ônibus)")
    args = ap.parse_args()

    if not os.path.exists(args.routes):
        raise SystemExit(f"routes.txt não encontrado: {args.routes}")

    rows = read_routes(args.routes)
    buses, totals = filter_buses(rows)
    wrote = write_route_ids(buses, args.out_ids)

    print(f"Total em routes.txt: {len(rows)}")
    for k in sorted(totals.keys()):
        print(f"  route_type {k or '(vazio)'}: {totals[k]}")
    print(f"Ônibus (route_type=={BUS_TYPE}) mantidos: {len(buses)}")
    print(f"Lista gerada: {args.out_ids} (itens: {wrote})")

    # falha se não encontrou nenhuma rota de ônibus (útil para CI/DAG)
    if wrote == 0:
        raise SystemExit(2)

if __name__ == "__main__":
    main()
