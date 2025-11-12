from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

spark = (
    SparkSession.builder.appName("SilverToGold_dim_linha")
    .getOrCreate()
)
print("✅ SparkSession inicializada")


# PATHS
today = "data_ref=" + datetime.today().strftime('%Y-%m-%d')

SILVER_LINHAS_PATH = f"s3a://silver/linhas/{today}"
SILVER_ROUTES_PATH = "s3a://silver/gtfs/routes/"
GOLD_DIM_LINHA_PATH = "s3a://gold/dim_linha/"

print(f"📂 Lendo Silver Linhas: {SILVER_LINHAS_PATH}")
print(f"📂 Lendo Silver GTFS Routes: {SILVER_ROUTES_PATH}")

# Leitura dos deltas
df_api = spark.read.format("delta").load(SILVER_LINHAS_PATH).alias("api")
df_gtfs = spark.read.format("delta").load(SILVER_ROUTES_PATH).alias("gtfs")

dim_linha = (
    df_api
    .filter(F.col("sentido_linha") == 1) # Filtrar apenas sentido de ida pra evitar duplicadas
    .join(
        df_gtfs,
        F.col("api.letreiro") == F.col("gtfs.letreiro"),
        "left"
    )
    .withColumn(
        "regiao",
        F.when(F.col("gtfs.route_color") == "509E2F", "Área 1 - Noroeste")
         .when(F.col("gtfs.route_color") == "002F6C", "Área 2 - Norte")
         .when(F.col("gtfs.route_color") == "FFD100", "Área 3 - Nordeste")
         .when(F.col("gtfs.route_color") == "DA291C", "Área 4 - Leste")
         .when(F.col("gtfs.route_color") == "006341", "Área 5 - Sudeste")
         .when(F.col("gtfs.route_color") == "0082BA", "Área 6 - Sul")
         .when(F.col("gtfs.route_color") == "782F40", "Área 7 - Sudoeste")
         .when(F.col("gtfs.route_color") == "FF671F", "Área 8 - Oeste")
         .otherwise("Outros")
    )
    # capitaliza os terminais
    .withColumn("terminal_origem", F.initcap(F.col("api.terminal_principal")))
    .withColumn("terminal_destino", F.initcap(F.col("api.terminal_secundario")))
    # colunas finais
    .select(
        F.col("api.codigo_linha"),
        F.col("api.letreiro"),
        F.col("api.modo_circular"),
        F.col("regiao"),
        F.col("terminal_origem"),
        F.col("terminal_destino"),
        F.current_timestamp().alias("transform_gold_timestamp")
    )
)

# Salvar no minio
(
    dim_linha
        .write.format("delta")
        .mode("overwrite")
        .save(GOLD_DIM_LINHA_PATH)
)
print(f"dim_linha salvo em {GOLD_DIM_LINHA_PATH}")

spark.stop()
print("SparkSession encerrada.")