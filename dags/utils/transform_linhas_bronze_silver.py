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
    .withColumnRenamed("lc", "modo_circular")  
    .withColumnRenamed("lt", "letreiro_1")
    .withColumnRenamed("sl", "sentido_linha")  
    .withColumnRenamed("tl", "letreiro_2")
    .withColumnRenamed("tp", "terminal_principal")
    .withColumnRenamed("ts", "terminal_secundario")
    # Normalizar terminais
    .withColumn("terminal_principal", F.lower(F.expr("STRIP_ACCENTS(terminal_principal)")))
    .withColumn("terminal_secundario", F.lower(F.expr("STRIP_ACCENTS(terminal_secundario)")))
    # Combinar letreiro_1 and letreiro_2 into letreiro
    .withColumn("letreiro", F.concat(F.col("letreiro_1"), F.lit("-"), F.col("letreiro_2")))
    .withColumn("data_ref", F.current_date())
    .withColumn("ingest_timestamp", F.current_timestamp())
    # Dropar colunas temporarias
    .drop("letreiro_1") 
    .drop("letreiro_2") 
    # Ordenar
    .select(
        "codigo_linha", 
        "modo_circular", 
        "letreiro", 
        "sentido_linha", 
        "terminal_principal", 
        "terminal_secundario", 
        "data_ref", 
        "ingest_timestamp"
    )
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
