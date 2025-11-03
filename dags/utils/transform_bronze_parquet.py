from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, to_date, current_timestamp, when, lit
)
from pyspark.sql.types import *
from datetime import datetime

# ================================================================
# 📘 BRONZE → SILVER (PARQUET VERSION)
# ================================================================
# Lê os JSONs brutos da camada Bronze (SPTrans API),
# transforma, limpa e grava na camada Silver como Parquet,
# particionando por codigo_linha e data_ref.
# ================================================================

# ---------------------
# 1️⃣ Inicializa Spark
# ---------------------
spark = (
    SparkSession.builder.appName("BronzeToSilver_Parquet")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .getOrCreate()
)

# ---------------------
# 2️⃣ Caminhos
# ---------------------
today = datetime.now().strftime("%Y/%m/%d")
BRONZE_PATH = f"s3a://bronze/posicao/{today}/"
SILVER_PATH = "s3a://silver/posicao/"

print(f"📥 Lendo Bronze de: {BRONZE_PATH}")
print(f"💾 Gravando Silver em: {SILVER_PATH}")

# ---------------------
# 3️⃣ Schema explícito
# ---------------------
schema = StructType([
    StructField("hr", StringType(), True),
    StructField("l", ArrayType(
        StructType([
            StructField("c", StringType(), True),
            StructField("cl", IntegerType(), True),
            StructField("sl", IntegerType(), True),
            StructField("lt0", StringType(), True),
            StructField("lt1", StringType(), True),
            StructField("qv", IntegerType(), True),
            StructField("vs", ArrayType(
                StructType([
                    StructField("p", IntegerType(), True),
                    StructField("a", BooleanType(), True),
                    StructField("ta", StringType(), True),
                    StructField("py", DoubleType(), True),
                    StructField("px", DoubleType(), True),
                ])
            ), True)
        ])
    ), True)
])

# ---------------------
# 4️⃣ Leitura Bronze
# ---------------------
df_raw = spark.read.option("multiline", True).schema(schema).json(BRONZE_PATH)

if df_raw.isEmpty():
    print("⚠️ Nenhum arquivo encontrado no Bronze.")
    spark.stop()
    exit(0)

# ---------------------
# 5️⃣ Explosão e limpeza
# ---------------------
df_exploded = (
    df_raw
    .withColumn("linha", explode(col("l")))
    .withColumn("veiculo", explode(col("linha.vs")))
    .filter(col("linha.cl").isNotNull())
)

# Classifica linhas regulares vs técnicas (ex: GUIN-10, TESTE)
df_exploded = df_exploded.withColumn(
    "tipo_linha",
    when((col("linha.cl") < 1000) | (col("linha.c").rlike("GUIN|TEST|TST")), lit("tecnica"))
    .otherwise(lit("regular"))
)

# Filtra apenas linhas regulares
df_exploded = df_exploded.filter(col("tipo_linha") == "regular")

# ---------------------
# 6️⃣ Seleção e enriquecimento
# ---------------------
df_clean = (
    df_exploded.select(
        col("linha.c").alias("codigo_linha_texto"),
        col("linha.cl").cast("int").alias("codigo_linha"),
        col("tipo_linha"),
        col("linha.sl").alias("sentido"),
        col("linha.lt0").alias("terminal_inicial"),
        col("linha.lt1").alias("terminal_final"),
        col("veiculo.p").alias("codigo_veiculo"),
        col("veiculo.a").alias("acessibilidade"),
        to_timestamp(col("veiculo.ta")).alias("ultima_atualizacao"),
        col("veiculo.py").alias("latitude"),
        col("veiculo.px").alias("longitude"),
        to_timestamp(col("hr")).alias("hora_referencia"),
    )
    .dropDuplicates(["codigo_veiculo", "hora_referencia"])
    .withColumn("data_ref", to_date(col("ultima_atualizacao")))
    .withColumn("data_coleta", to_date(col("hora_referencia")))
    .withColumn("data_ingestao", to_date(current_timestamp()))
    .withColumn("ingest_timestamp", current_timestamp())
)

# ---------------------
# 7️⃣ Escrita Silver (Parquet)
# ---------------------
(
    df_clean
    .write
    .format("parquet")
    .mode("append")
    .partitionBy("codigo_linha", "data_ref")
    .save(SILVER_PATH)
)

print("✅ Transformação concluída e salva na camada Silver (Parquet).")

# ---------------------
# 8️⃣ Debug opcional
# ---------------------
df_clean.groupBy("codigo_linha").count().orderBy("codigo_linha").show(10)
spark.stop()