# duckdb-minio.py
import os
import duckdb

def load_duckdb():
    """
    Lê Parquet do MinIO usando DuckDB e cria tabela local.
    """
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio.local:9000")
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "miniouser")
    MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "miniopass")
    MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "false").lower() == "true"
    BUCKET = os.environ.get("S3_BUCKET", "sptrans-test")
    PARQUET_PATH = os.environ.get("PARQUET_PATH", "trusted/test")
    DUCKDB_FILE = os.environ.get("DUCKDB_FILE", "/tmp/trusted_test.duckdb")

    conn = duckdb.connect(DUCKDB_FILE)
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute(f"SET s3_use_ssl={'true' if MINIO_USE_SSL else 'false'};")
    conn.execute("SET s3_url_style='path';")

    parquet_glob = f"s3://{BUCKET}/{PARQUET_PATH}/*.parquet"
    print("Tentando ler:", parquet_glob)

    conn.execute(f"CREATE OR REPLACE TABLE trusted_test AS SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=True);")
    df = conn.execute("SELECT count(*) as total FROM trusted_test").fetchdf()
    print("Número de linhas lidas:", df["total"].iloc[0])

    conn.close()

if __name__ == "__main__":
    load_duckdb()
