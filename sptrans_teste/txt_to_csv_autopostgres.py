import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Configurações do Postgres
# -----------------------------
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")  # host opcional
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)         # porta opcional

# Conexão com o Postgres
engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# -----------------------------
# Pastas de entrada e saída
# -----------------------------
input_txt_folder = Path("./txts")   # pasta com arquivos .txt
output_csv_folder = Path("./csvs")  # pasta para salvar CSVs
output_csv_folder.mkdir(parents=True, exist_ok=True)

# Delimitador padrão (ajuste se necessário)
delimiter = ';'

# -----------------------------
# Função principal
# -----------------------------
for txt_file in input_txt_folder.glob("*.txt"):
    try:
        # Caminho do CSV de saída
        csv_file = output_csv_folder / f"{txt_file.stem}.csv"

        # 1️⃣ Converte TXT → CSV
        with open(txt_file, "r", encoding="utf-8") as f:
            lines = [line.strip().split(delimiter) for line in f if line.strip()]

        # Salva CSV
        df = pd.DataFrame(lines[1:], columns=lines[0])  # assume primeira linha = cabeçalho
        df.to_csv(csv_file, index=False, sep=delimiter, encoding="utf-8")
        print(f"✅ Convertido: {txt_file.name} → {csv_file.name}")

        # 2️⃣ Carrega CSV → tabela Postgres
        table_name = txt_file.stem  # nome da tabela = nome do arquivo sem extensão
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"🚀 Carregado: '{csv_file.name}' → tabela '{table_name}'")

    except Exception as e:
        print(f"⚠️ Erro com '{txt_file.name}': {e}")

print("\n🎉 Processamento concluído!")

