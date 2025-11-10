import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Helper - remover acentos
def strip_accents(txt: str):
    if txt is None:
        return None
    txt_norm = unicodedata.normalize('NFD', txt)
    return ''.join(c for c in txt_norm if unicodedata.category(c) != 'Mn')

# UDF pra tirar acentos
STRIP_ACCENTS = F.udf(strip_accents, T.StringType())

# Spark: Configurações default em spark/spark-defaults.conf
spark = (
    SparkSession.builder.appName("BronzeToSilver_GTFS_Delta")
    .getOrCreate()
)
print("✅ SparkSession inicializada")

# -----------------------------
# Paths
# -----------------------------
BRONZE_PATH = f"s3a://bronze/gtfs/"
SILVER_PATH = "s3a://silver/gtfs/"

# -------------------------
# ROUTES
# -------------------------
print("▶ Transforming ROUTES ...")
schema = T.StructType([
    T.StructField("route_id",          T.StringType(), True),
    T.StructField("agency_id",         T.IntegerType(), True),
    T.StructField("route_short_name",  T.StringType(), True),
    T.StructField("route_long_name",   T.StringType(), True),
    T.StructField("route_type",        T.IntegerType(), True),
    T.StructField("route_color",       T.StringType(), True),
    T.StructField("route_text_color",  T.StringType(), True),
])

df = (spark.read.option("header", True).schema(schema).csv(f"{BRONZE_PATH}routes.txt"))

# Filter: somente ônibus (route_type=3)
df = df.filter(F.col("route_type") == 3)

# Select & normalize
# - letreiro de route_id, colunas iguais
# - nome_publico normalizado (lower + acentos removidos)
df = (
    df
    .select("route_id", "route_long_name", "route_color", "route_text_color")
    .withColumnRenamed("route_id", "letreiro")
    .withColumn("nome_publico", F.lower(STRIP_ACCENTS(F.col("route_long_name"))))
    .drop("route_long_name")
)

# Split terminals por "-"
df = (
    df
    .withColumn("nome_publico_split", F.split(F.regexp_replace("nome_publico", "–", "-"), "-"))
    .withColumn("terminal_inicial", F.trim(F.col("nome_publico_split").getItem(0)))
    .withColumn("terminal_final",   F.trim(F.col("nome_publico_split").getItem(1)))
    .drop("nome_publico_split")
)

# Salvar
(
    df.repartition(1)
        .write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f"{SILVER_PATH}routes")
)
print("✅ routes ->", f"{SILVER_PATH}routes")

# -----------------------------
# STOPS
# -----------------------------
print("▶ Transforming STOPS ...")
schema = T.StructType([
    T.StructField("stop_id",    T.StringType(), True),
    T.StructField("stop_name",  T.StringType(), True),
    T.StructField("stop_desc",  T.StringType(), True),
    T.StructField("stop_lat",   T.DoubleType(), True),
    T.StructField("stop_lon",   T.DoubleType(), True),
])

df = (spark.read.option("header", True).schema(schema).csv(f"{BRONZE_PATH}stops.txt"))

df = (
    df
    .withColumn("stop_name", F.lower(STRIP_ACCENTS(F.col("stop_name"))))
    .withColumnRenamed("stop_id", "id_parada")
    .withColumnRenamed("stop_name", "nome_parada")
    .withColumnRenamed("stop_lat", "latitude")
    .withColumnRenamed("stop_lon", "longitude")
    .drop("stop_desc")
)

(
    df.repartition(1)
        .write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f"{SILVER_PATH}stops")
)
print("✅ stops ->", f"{SILVER_PATH}stops")

# -----------------------------
# TRIPS
# -----------------------------
print("▶ Transforming TRIPS ...")
schema = T.StructType([
    T.StructField("route_id",       T.StringType(), True),
    T.StructField("service_id",     T.StringType(), True),
    T.StructField("trip_id",        T.StringType(), True),
    T.StructField("trip_headsign",  T.StringType(), True),
    T.StructField("direction_id",   T.IntegerType(), True),
    T.StructField("shape_id",       T.IntegerType(), True),
])

df = (spark.read.option("header", True).schema(schema).csv(f"{BRONZE_PATH}trips.txt"))

df = (
    df
    .withColumn("trip_headsign", F.lower(STRIP_ACCENTS(F.col("trip_headsign"))))
    .withColumnRenamed("route_id", "letreiro")
    .withColumnRenamed("service_id", "dias_funcionamento")
    .withColumnRenamed("trip_headsign", "destino")
    .withColumnRenamed("direction_id", "sentido")
    .select("trip_id", "letreiro", "sentido", "destino", "shape_id", "dias_funcionamento")
    .dropDuplicates(["trip_id"])
)

(
    df.repartition(1)
        .write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f"{SILVER_PATH}trips")
)
print("✅ trips ->", f"{SILVER_PATH}trips")

# -----------------------------
# STOP_TIMES
# -----------------------------
print("▶ Transforming STOP_TIMES ...")
schema = T.StructType([
    T.StructField("trip_id",         T.StringType(), True),
    T.StructField("arrival_time",    T.StringType(), True),
    T.StructField("departure_time",  T.StringType(), True),
    T.StructField("stop_id",         T.IntegerType(), True),
    T.StructField("stop_sequence",   T.IntegerType(), True),
])

df = (spark.read.option("header", True).schema(schema).csv(f"{BRONZE_PATH}stop_times.txt"))

df = (
    df
    .withColumnRenamed("arrival_time", "hora_chegada")
    .withColumnRenamed("departure_time", "hora_partida")
    .withColumnRenamed("stop_sequence", "sequencia_parada")
)

(
    df.repartition(1)
        .write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f"{SILVER_PATH}stop_times")
)
print("✅ stop_times ->", f"{SILVER_PATH}stop_times")

# -----------------------------
# SHAPES
# -----------------------------
print("▶ Transforming SHAPES ...")
schema = T.StructType([
    T.StructField("shape_id",             T.StringType(), True),
    T.StructField("shape_pt_lat",         T.DoubleType(), True),
    T.StructField("shape_pt_lon",         T.DoubleType(), True),
    T.StructField("shape_pt_sequence",    T.IntegerType(), True),
    T.StructField("shape_dist_traveled",  T.FloatType(), True),
])

df = (spark.read.option("header", True).schema(schema).csv(f"{BRONZE_PATH}shapes.txt"))

df = (
    df
    .withColumnRenamed("shape_pt_lat", "latitude")
    .withColumnRenamed("shape_pt_lon", "longitude")
    .withColumnRenamed("shape_pt_sequence", "sequencia_ponto")
    .withColumn("distancia_acumulada", F.round(F.col("shape_dist_traveled").cast("double"), 2))
    .drop("shape_dist_traveled")
)

(
    df.repartition(1)
        .write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f"{SILVER_PATH}shapes")
)
print("✅ shapes ->", f"{SILVER_PATH}shapes")

spark.stop()
print("SparkSession encerrada.")