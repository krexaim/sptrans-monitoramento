import duckdb
import os

print("💾 Criando/atualizando banco local: sptrans.duckdb")

# Caminho do arquivo DuckDB (volume compartilhado entre Airflow e Metabase)
DB_PATH = "/opt/airflow/dags/data/metabase/sptrans.duckdb"

# Diretórios S3 (MinIO)
paths = {
    "dim_linha": "s3://gold/dim_linha/*.parquet",
    "dim_parada": "s3://gold/dim_parada/*.parquet",
    "fato_posicao": "s3://gold/fato_posicao/**/*.parquet",
    # Caso você tenha KPIs futuros:
    # "kpi_operacional_diario": "s3://gold/kpis/kpi_operacional_diario/*.parquet",
}

# Conecta ou cria o banco DuckDB
con = duckdb.connect(DB_PATH)
print(f"📂 Conectado ao banco: {DB_PATH}")

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
        print(f"✅ Tabela gold.{name} criada/atualizada com sucesso.")
    except Exception as e:
        print(f"⚠️ Erro ao processar {name}: {e}")

# Verifica tabelas criadas
print("\n📊 Tabelas disponíveis:")
print(con.execute("SHOW TABLES FROM gold;").fetchall())

con.close()
print(f"\n🎉 Banco DuckDB atualizado em: {DB_PATH}")
