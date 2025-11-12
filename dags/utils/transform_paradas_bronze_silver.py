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
# SparkSession
# --------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("BronzeToSilver_Paradas")
    .getOrCreate()
)
print("✅ SparkSession inicializada (paradas)")

# --------------------------------------------------------------------
# Caminhos
# --------------------------------------------------------------------
today = datetime.now().strftime("%Y/%m/%d")
BRONZE_PATH = f"s3a://bronze/paradas/{today}/"
SILVER_PATH = "s3a://silver/paradas/"
print(f"📂 Lendo Bronze: {BRONZE_PATH}")

# --------------------------------------------------------------------
# Leitura Bronze (JSON) 
# --------------------------------------------------------------------
df_raw = spark.read.option("multiline", True).json(BRONZE_PATH)

# Explode stops → 1 linha por parada
df = (
    df_raw
    .filter(F.size("stops") > 0)  # remove linhas sem paradas
    .withColumn("stop", F.explode("stops"))
    .select(
        F.col("route_id").alias("codigo_linha").cast("int"),
        F.col("stop.cp").alias("codigo_parada").cast("int"),
        F.col("stop.np").alias("nome_parada"),
        F.col("stop.ed").alias("endereco"),
        F.col("stop.py").alias("latitude"),
        F.col("stop.px").alias("longitude"),
    )
    .withColumn("nome_parada", F.lower(STRIP_ACCENTS(F.col("nome_parada"))))
    .withColumn("endereco", F.lower(STRIP_ACCENTS(F.col("endereco"))))
    .withColumn("data_ref", F.current_date())
    .withColumn("ingest_timestamp", F.current_timestamp())
)

# --------------------------------------------------------------------
# Escrita em Delta (Silver)
# --------------------------------------------------------------------
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("data_ref")
    .save(SILVER_PATH)
)

print("✅ Silver de PARADAS gravado em Delta em:", SILVER_PATH)
print(f"🚌 Total de linhas lidas: {df_raw.count()}")
print(f"📍 Paradas válidas (com stops): {df.count()}")

spark.stop()
print("✅ SparkSession finalizada (paradas)")
