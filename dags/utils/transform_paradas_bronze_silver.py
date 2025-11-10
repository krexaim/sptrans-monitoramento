from pyspark.sql import SparkSession, functions as F, types as T

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
BRONZE_PATH = "s3a://bronze/paradas"
SILVER_PATH = "s3a://silver/paradas"   # em Delta

bronze_pattern = f"{BRONZE_PATH.rstrip('/')}/*/*/*/*.json"

# --------------------------------------------------------------------
# Leitura Bronze - JSON “mapa”: { "2486": [], "2497": [{...}, ...], ... }
# Usando read.json (multiLine) e depois convertendo tudo para um MapType
# --------------------------------------------------------------------
df_json = (
    spark.read
    .option("multiLine", True)  # arquivo é um único objeto grande
    .json(bronze_pattern)
    .withColumn("path", F.input_file_name())
)

# df_json terá 1 linha por arquivo, com uma coluna por código de linha (2497, 2486, 35254, ...)
# Vamos juntar tudo em um único JSON de volta, e depois parsear com schema de MAPA.

cols_sem_path = [c for c in df_json.columns if c != "path"]

df_raw = df_json.select(
    "path",
    F.to_json(F.struct(*[F.col(c) for c in cols_sem_path])).alias("raw")
)

# Schema do mapa: { linha (string) -> array de paradas }
schema_paradas_mapa = T.MapType(
    T.StringType(),
    T.ArrayType(
        T.StructType([
            T.StructField("cp", T.LongType(), True),     # id do ponto
            T.StructField("np", T.StringType(), True),   # nome do ponto
            T.StructField("ed", T.StringType(), True),   # endereço / descrição
            T.StructField("py", T.DoubleType(), True),   # latitude
            T.StructField("px", T.DoubleType(), True),   # longitude
        ])
    )
)

df_mapa = df_raw.select(
    "path",
    F.from_json("raw", schema_paradas_mapa).alias("paradas_mapa")
)

# --------------------------------------------------------------------
# Explode: 1 linha por (linha, parada)
# --------------------------------------------------------------------
df_explodido = (
    df_mapa
    .select(
        "path",
        F.explode("paradas_mapa").alias("route_id", "lista_paradas")  # route_id = código da linha (chave do mapa)
    )
    .withColumn("parada", F.explode("lista_paradas"))
)

# Flatten dos campos da parada
df_flat = (
    df_explodido
    .select(
        "path",
        "route_id",
        F.col("parada.cp").alias("stop_id"),
        F.col("parada.np").alias("stop_name"),
        F.col("parada.ed").alias("stop_desc"),
        F.col("parada.py").alias("stop_lat"),
        F.col("parada.px").alias("stop_lon"),
    )
)

# --------------------------------------------------------------------
# data_ref e ingest_timestamp (padronização com GTFS / outros scripts)
# --------------------------------------------------------------------
df_flat = (
    df_flat
    .withColumn("ano",  F.regexp_extract("path", r"/paradas/(\\d{4})/(\\d{2})/(\\d{2})/", 1).cast("int"))
    .withColumn("mes",  F.regexp_extract("path", r"/paradas/(\\d{4})/(\\d{2})/(\\d{2})/", 2).cast("int"))
    .withColumn("dia",  F.regexp_extract("path", r"/paradas/(\\d{4})/(\\d{2})/(\\d{2})/", 3).cast("int"))
    .withColumn("data_ref", F.to_date(F.concat_ws("-", "ano", "mes", "dia")))
    .withColumn("ingest_timestamp", F.current_timestamp())
)

# Se no seu código antigo você tinha dt/ingest_ts:
# df_flat = df_flat.withColumnRenamed("dt", "data_ref").withColumnRenamed("ingest_ts", "ingest_timestamp")

# --------------------------------------------------------------------
# Seleciona colunas finais no padrão GTFS-ish
# --------------------------------------------------------------------
colunas_final = [
    "route_id",          # código da linha (chave do JSON)
    "stop_id",
    "stop_name",
    "stop_desc",
    "stop_lat",
    "stop_lon",
    "data_ref",
    "ingest_timestamp",
]

df_silver = df_flat.select(*colunas_final)

# --------------------------------------------------------------------
# Escrita em Delta (Silver)
# --------------------------------------------------------------------
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("data_ref")
    .save(SILVER_PATH)
)

print("✅ Silver de PARADAS gravado em Delta em:", SILVER_PATH)

spark.stop()
print("✅ SparkSession finalizada (paradas)")
