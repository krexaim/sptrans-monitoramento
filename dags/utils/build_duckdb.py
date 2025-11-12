import duckdb
import shutil
import os

print("💾 Criando/atualizando banco local: sptrans.duckdb")

# Caminho do arquivo DuckDB (volume compartilhado entre Airflow e Metabase)
FINAL_PATH = "/opt/airflow/dags/data/sptrans.duckdb"
TEMP_PATH = "/opt/airflow/dags/data/sptrans_temp.duckdb"

# Diretórios S3 (MinIO)
paths = {
    "dim_linha": "s3://gold/dim_linha/*.parquet",
    "dim_parada": "s3://gold/dim_parada/*.parquet",
    "fato_posicao": "s3://gold/fato_posicao/**/*.parquet",
    # Caso tenha KPIs futuros:
    # "kpi_operacional_diario": "s3://gold/kpis/kpi_operacional_diario/*.parquet",
}

# Garante que diretório existe
os.makedirs(os.path.dirname(FINAL_PATH), exist_ok=True)

# Conexão temporária
con = duckdb.connect(TEMP_PATH)
con.execute("INSTALL httpfs; LOAD httpfs;")
print(f"📂 Conectado ao banco: {TEMP_PATH}")

# Habilita leitura do MinIO (via S3)
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("""
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# Cria schema gold se não existir
con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

# Lê e recria cada tabela do MinIO dentro do DuckDB
for name, path in paths.items():
    print(f"📦 Lendo {name} de {path}...")
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE gold.{name} AS
            SELECT * FROM read_parquet('{path}');
        """)
        print(f"✅ Tabela gold.{name} criada/atualizada.")
    except Exception as e:
        print(f"⚠️ Erro ao processar {name}: {e}")

# Fecha o banco temporário
con.close()

# Substitui o arquivo antigo (movimento atômico)
try:
    shutil.move(TEMP_PATH, FINAL_PATH)
    size = os.path.getsize(FINAL_PATH)
    mtime = os.path.getmtime(FINAL_PATH)
    print(f"📏 Novo arquivo: {size/1024/1024:.2f} MB | Última modificação: {mtime}")
    print(f"🎉 Banco atualizado com sucesso → {FINAL_PATH}")
except Exception as e:
    print(f"❌ Falha ao substituir arquivo final: {e}")