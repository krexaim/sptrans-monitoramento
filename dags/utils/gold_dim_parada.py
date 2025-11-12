from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

spark = (
    SparkSession.builder.appName("SilverToGold_dim_parada")
    .getOrCreate()
)
print("✅ SparkSession inicializada")

# Paths
SILVER_STOPS = "s3a://silver/gtfs/stops/"
SILVER_STOP_TIMES = "s3a://silver/gtfs/stop_times/"
SILVER_TRIPS = "s3a://silver/gtfs/trips/"
SILVER_PARADAS_API = "s3a://silver/paradas/"
GOLD_DIM_PARADA = "s3a://gold/dim_parada/"

# Leitura das tabelas
stops = spark.read.format("delta").load(SILVER_STOPS)
stop_times = spark.read.format("delta").load(SILVER_STOP_TIMES)
trips = spark.read.format("delta").load(SILVER_TRIPS)
paradas_api = spark.read.format("delta").load(SILVER_PARADAS_API)

# 1. join stop_times -> trips para saber qual linha atende cada parada
linha_por_parada = (
    stop_times
    .join(trips, "trip_id", "left")
    .select(
        F.col("stop_id").alias("codigo_parada"),
        F.col("letreiro")
    )
    # Filtrar por IDs numéricos (paradas de onibus)
    .filter(F.col("codigo_parada").rlike("^[0-9]+$"))
    # Remover linhas de metrô e CPTM
    .filter(~F.lower(F.col("letreiro")).rlike("metr|cptm"))
    .distinct()
)

# 2. agrega lista e contagem de linhas por parada
agg_parada = (
    linha_por_parada
    .groupBy("codigo_parada")
    .agg(
        F.collect_set("letreiro").alias("linhas_atendidas"),
        F.size(F.collect_set("letreiro")).alias("num_linhas_atendidas")
    )
)

# 3. base GTFS (stops)
dim_base = (
    stops
    .select(
        F.col("id_parada").alias("codigo_parada"),
        F.col("nome_parada"),
        F.col("latitude"),
        F.col("longitude")
    )
)

# 4️. join com paradas da API (para marcar se é corredor)
dim_final = (
    dim_base.alias("gtfs")
    .join(
        paradas_api.select("codigo_parada").distinct().alias("api"),
        F.col("gtfs.codigo_parada") == F.col("api.codigo_parada"),
        "left"
    )
    .join(agg_parada.alias("agg"), "codigo_parada", "left")
    .withColumn("is_corredor", F.col("api.codigo_parada").isNotNull())
    .withColumn(
        "tipo_parada",
        F.when(F.col("num_linhas_atendidas").isNull() | (F.col("num_linhas_atendidas") == 0),
               F.lit("metro/cptm"))
         .otherwise(F.lit("onibus"))
    )
    .filter(F.col("tipo_parada") == "onibus")
    # normaliza os nomes
    .withColumn("nome_parada", F.initcap(F.col("nome_parada")))
    # colunas finais
    .select(
        F.col("gtfs.codigo_parada").cast("int"),
        "nome_parada",
        "latitude",
        "longitude",
        "num_linhas_atendidas",
        "is_corredor",
        "linhas_atendidas",
        F.current_timestamp().alias("transform_gold_timestamp")
    )
)
# 5. Escrever na gold
(
    dim_final.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_DIM_PARADA)
)

# Logs básicos
total = dim_final.count()
corredor = dim_final.filter(F.col("is_corredor") == True).count()
print(f"✅ Gold: {total} paradas totais | 🚌 {corredor} em corredores ({corredor/total*100:.2f}%)")
print("💾 Gravado em:", GOLD_DIM_PARADA)

spark.stop()
print("SparkSession encerrada.")