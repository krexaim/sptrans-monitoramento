from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, to_timestamp
from pyspark.sql.types import *
from delta.tables import DeltaTable
import os

# ---------------------
# Configurações MinIO
# ---------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_LOCAL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BRONZE_PATH = "s3a://bronze/posicao/2025/*/*/"
SILVER_PATH = "s3a://silver/posicao_delta/"

# ---------------------
# Inicializa Spark com Delta
# ---------------------
spark = (
    SparkSession.builder.appName("Transform_Posicao_to_Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .getOrCreate()
)

# ---------------------
# Schema explícito
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
            StructField("vs", ArrayType(
                StructType([
                    StructField("p", IntegerType(), True),
                    StructField("a", BooleanType(), True),
                    StructField("ta", StringType(), True),
                    StructField("py", DoubleType(), True),
                    StructField("px", DoubleType(), True)
                ])
            ))
        ])
    ))
])

# ---------------------
# Leitura Bronze
# ---------------------
df_raw = spark.read.option("multiline", True).schema(schema).json(BRONZE_PATH)

df_exploded = (
    df_raw
    .withColumn("linha", explode(col("l")))
    .withColumn("veiculo", explode(col("linha.vs")))
)

df_clean = (
    df_exploded.select(
        col("linha.c").alias("codigo_linha_texto"),
        col("linha.cl").alias("codigo_linha"),
        col("linha.sl").alias("sentido"),
        col("linha.lt0").alias("terminal_inicial"),
        col("linha.lt1").alias("terminal_final"),
        col("veiculo.p").alias("codigo_veiculo"),
        col("veiculo.a").alias("acessibilidade"),
        to_timestamp(col("veiculo.ta")).alias("ultima_atualizacao"),
        col("veiculo.py").alias("latitude"),
        col("veiculo.px").alias("longitude"),
        to_timestamp(col("hr")).alias("hora_referencia")
    )
    .withColumn("ingest_timestamp", current_timestamp())
    .dropDuplicates(["codigo_veiculo", "hora_referencia"])
)

# ---------------------
# Escrita em formato Delta (upsert incremental)
# ---------------------
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        delta_table.alias("t")
        .merge(df_clean.alias("s"),
               "t.codigo_veiculo = s.codigo_veiculo AND t.hora_referencia = s.hora_referencia")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("✅ Transformação para Silver (Delta) concluída.")
spark.stop()
