from pyspark.sql import SparkSession, functions as F, types as T

# --------------------------------------------------------------------
# SparkSession - usando configs do spark-defaults.conf (MinIO, Delta, etc)
# --------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("BronzeToSilver_Linhas")
    .getOrCreate()
)

print("✅ SparkSession inicializada (linhas)")

# --------------------------------------------------------------------
# Caminhos no Data Lake
# ajuste se seus caminhos forem diferentes
# --------------------------------------------------------------------
BRONZE_PATH = "s3a://bronze/linhas"
SILVER_PATH = "s3a://silver/linhas"   # em Delta

# --------------------------------------------------------------------
# Leitura Bronze (JSON) - raiz é array de objetos
# --------------------------------------------------------------------
# Exemplo de arquivos:
#  s3a://bronze/linhas/2025/11/07/linhas_20251107_032544.json
# Pega todos os dias/pastas:
bronze_pattern = f"{BRONZE_PATH.rstrip('/')}/*/*/*/*.json"

df_raw = (
    spark.read
    .option("multiLine", True)  # arquivo é um array JSON
    .json(bronze_pattern)
)

# Adiciona o caminho do arquivo para extrair data
df_raw = df_raw.withColumn("input_file", F.input_file_name())

# --------------------------------------------------------------------
# Extração de data_ref (equivalente ao antigo dt)
# a partir do caminho: .../linhas/AAAA/MM/DD/arquivo.json
# --------------------------------------------------------------------
df = (
    df_raw
    .withColumn("ano",  F.regexp_extract("input_file", r"/linhas/(\\d{4})/(\\d{2})/(\\d{2})/", 1).cast("int"))
    .withColumn("mes",  F.regexp_extract("input_file", r"/linhas/(\\d{4})/(\\d{2})/(\\d{2})/", 2).cast("int"))
    .withColumn("dia",  F.regexp_extract("input_file", r"/linhas/(\\d{4})/(\\d{2})/(\\d{2})/", 3).cast("int"))
    .withColumn("data_ref", F.to_date(F.concat_ws("-", "ano", "mes", "dia")))
    .withColumn("ingest_timestamp", F.current_timestamp())
)

# --------------------------------------------------------------------
# Renomeia colunas “cruas” da API para algo mais legível
# (ajuste os nomes se você já estiver usando outro padrão)
# --------------------------------------------------------------------
df = (
    df
    .withColumnRenamed("cl", "codigo_linha_sptrans")   # id interno da SPTrans
    .withColumnRenamed("lc", "linha_circular")         # bool
    .withColumnRenamed("lt", "letreiro")               # ex: 8000
    .withColumnRenamed("sl", "sentido")                # 1 / 2 etc
    .withColumnRenamed("tl", "tipo_linha")             # se houver
    .withColumnRenamed("tp", "terminal_principal")     # se houver
    .withColumnRenamed("ts", "terminal_secundario")    # se houver
)

# Se você já tinha colunas dt e ingest_ts antes:
# df = df.withColumnRenamed("dt", "data_ref").withColumnRenamed("ingest_ts", "ingest_timestamp")

# Seleciona ordem final de colunas (ajuste conforme necessidade)
colunas_final = [
    "codigo_linha_sptrans",
    "linha_circular",
    "letreiro",
    "sentido",
    "tipo_linha",
    "terminal_principal",
    "terminal_secundario",
    "data_ref",
    "ingest_timestamp",
]

df_silver = df.select(*[c for c in colunas_final if c in df.columns])

# --------------------------------------------------------------------
# Escrita em Silver (Delta), particionado por data_ref
# --------------------------------------------------------------------
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")           # snapshot completo; se quiser, depois troca por overwrite por partição
    .partitionBy("data_ref")
    .save(SILVER_PATH)
)

print("✅ Silver de LINHAS gravado em Delta em:", SILVER_PATH)

spark.stop()
print("✅ SparkSession finalizada (linhas)")
