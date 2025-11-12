import duckdb
import shutil
import os

print("💾 Criando/atualizando banco local: sptrans.duckdb")

BASE_PATH = "/opt/airflow/dags/data/metabase"
TEMP_PATH = f"{BASE_PATH}/sptrans_temp.duckdb"
FINAL_PATH = f"{BASE_PATH}/sptrans.duckdb"

# MinIO endpoint
MINIO_ENDPOINT = "minio:9000"

# Delta/Parquet paths
paths = {
    "dim_linha": "s3://gold/dim_linha/",
    "dim_parada": "s3://gold/dim_parada/",
    "fato_posicao": "s3://gold/fato_posicao/",
}

# Ensure directory exists
os.makedirs(os.path.dirname(FINAL_PATH), exist_ok=True)

# Connect to DuckDB file
con = duckdb.connect(TEMP_PATH)
print(f"📂 Conectado ao banco: {TEMP_PATH}")

# Enable MinIO access (DuckDB credentials)
con.execute(f"""
    CREATE SECRET minio_s3 (
        TYPE s3,
        KEY_ID 'admin',
        SECRET 'minioadmin',
        REGION 'us-east-1',
        ENDPOINT '{MINIO_ENDPOINT}',
        USE_SSL false,
        URL_STYLE 'path'
    );
""")

# Create schema gold
con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

for name, path in paths.items():
    print(f"📦 Processando {name} de {path}...")

    # Try Delta first
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE gold.{name} AS
            SELECT * FROM delta_scan('{path}');
        """)
        print(f"✅ Lido como Delta → gold.{name}")
        continue
    except Exception as delta_err:
        print(f"⚠️ Delta falhou para {name}: {delta_err}")
        print("➡️ Tentando fallback com read_parquet...")

    # Fallback to Parquet glob
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE gold.{name} AS
            SELECT * FROM read_parquet('{path}*.parquet');
        """)
        print(f"✅ Lido como Parquet → gold.{name}")
    except Exception as parquet_err:
        print(f"❌ Falhou ao ler {name}: {parquet_err}")

# Close connection
con.close()

# Atomic replace
try:
    shutil.move(TEMP_PATH, FINAL_PATH)
    size = os.path.getsize(FINAL_PATH) / 1024 / 1024
    print(f"📏 Novo arquivo: {size:.2f} MB")
    print(f"🎉 Banco atualizado com sucesso → {FINAL_PATH}")
except Exception as e:
    print(f"❌ Falha ao substituir arquivo final: {e}")