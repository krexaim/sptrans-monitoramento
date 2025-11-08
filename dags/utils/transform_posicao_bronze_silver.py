from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = (
    SparkSession.builder.appName("BronzeToSilver_Delta")
    .getOrCreate()
)

print("✅ SparkSession inicializada")

# PATHS
today = datetime.now().strftime("%Y/%m/%d")
BRONZE_PATH = f"s3a://bronze/posicao/{today}/"
SILVER_PATH = "s3a://silver/posicao/"

print(f"📂 Lendo Bronze: {BRONZE_PATH}")

# Ler o json mais recente
files_df = spark.read.format("binaryFile").load(BRONZE_PATH)
files = [row.path for row in files_df.select("path").collect() if row.path.endswith(".json")]

if not files:
    print("⚠️ Nenhum arquivo JSON encontrado.")
    spark.stop()
    exit(0)

latest_file = sorted(files)[-1]
print(f"📦 Lendo arquivo mais recente: {latest_file}")

# ================================================================
# Transformação
# ================================================================
df_raw = spark.read.option("mode", "PERMISSIVE").json(latest_file)

if df_raw.isEmpty():
    print("⚠️ Arquivo vazio.")
    spark.stop()
    exit(0)

df = (
    df_raw
    .selectExpr("hr", "inline(l)")
    .selectExpr(
        "hr",
        "c as letreiro",
        "cl as codigo_linha",
        "sl as sentido",
        "lt0 as terminal_inicial",
        "lt1 as terminal_final",
        "qv",
        "inline(vs)"
    )
    .filter("codigo_linha IS NOT NULL AND NOT (codigo_linha < 1000 OR letreiro RLIKE 'GUIN|TEST|TST')")
    .select(
        "letreiro",
        "codigo_linha",
        "sentido",
        "terminal_inicial",
        "terminal_final",
        F.col("p").alias("codigo_veiculo"),
        F.col("a").alias("acessibilidade"),
        F.to_timestamp("ta").alias("ultima_atualizacao"),
        F.col("py").alias("latitude"),
        F.col("px").alias("longitude"),
        F.to_timestamp("hr").alias("hora_referencia"),
    )
    .dropDuplicates(["codigo_veiculo", "hora_referencia"])
    .withColumn("latitude", F.round("latitude", 6))
    .withColumn("longitude", F.round("longitude", 6))
    .withColumn("data_ref", F.to_date("ultima_atualizacao"))
    .withColumn("ingest_timestamp", F.current_timestamp())
)

# ================================================================
# Escrita incremental
# ================================================================
if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("🆕 Criando nova tabela Silver...")
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("data_ref")
        .save(SILVER_PATH)
    )
else:
    print("🔁 Aplicando merge incremental...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver_table.alias("tgt")
        .merge(
            df.alias("src"),
            "tgt.codigo_veiculo = src.codigo_veiculo AND tgt.hora_referencia = src.hora_referencia"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("✅ Transformação Bronze → Silver concluída.")
spark.stop()