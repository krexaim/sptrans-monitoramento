from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime
import unicodedata

# ------------------------------------------------------------
# Helper para remover acentos
# ------------------------------------------------------------
def strip_accents(txt: str):
    if txt is None:
        return None
    txt_norm = unicodedata.normalize('NFD', txt)
    return ''.join(c for c in txt_norm if unicodedata.category(c) != 'Mn')

STRIP_ACCENTS = F.udf(strip_accents, T.StringType())

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
today = datetime.now().strftime("%Y/%m/%d")
BRONZE_PATH = f"s3a://bronze/linhas/{today}/"
SILVER_PATH = "s3a://silver/linhas/"
print(f"📂 Lendo Bronze: {BRONZE_PATH}")

# --------------------------------------------------------------------
# Leitura Bronze (JSON) 
# --------------------------------------------------------------------
df = (
    spark.read
    .json(BRONZE_PATH)
)

# --------------------------------------------------------------------
# Renomeia colunas
# --------------------------------------------------------------------
df = (
    df
    .withColumnRenamed("cl", "codigo_linha")
    .withColumnRenamed("lc", "linha_circular")
    .withColumnRenamed("lt", "letreiro")
    .withColumnRenamed("sl", "sentido")
    .withColumnRenamed("tl", "tipo_linha")
    .withColumnRenamed("tp", "terminal_principal")
    .withColumnRenamed("ts", "terminal_secundario")
    .withColumn("terminal_principal", F.lower(STRIP_ACCENTS(F.col("terminal_principal"))))
    .withColumn("terminal_secundario", F.lower(STRIP_ACCENTS(F.col("terminal_secundario"))))
    .withColumn("data_ref", F.current_date())
    .withColumn("ingest_timestamp", F.current_timestamp())
)

# --------------------------------------------------------------------
# Escrita em Silver (Delta), particionado por data_ref
# --------------------------------------------------------------------
(
    df
    .write
    .format("delta")
    .mode("overwrite")         
    .partitionBy("data_ref")
    .save(SILVER_PATH)
)

print("✅ Silver de LINHAS gravado em Delta em:", SILVER_PATH)

spark.stop()
print("✅ SparkSession finalizada (linhas)")
