# utils/bronze_to_silver.py

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, explode, current_timestamp, to_date
)
from pyspark.sql import functions as F

def bronze_to_silver_linhas():
    today = datetime.now().strftime("%Y/%m/%d")
    BRONZE_PATH = f"s3a://bronze/linhas/{today}/"
    SILVER_PATH = "s3a://silver/dim_linhas/"

    spark = (
        SparkSession.builder.appName("BronzeToSilver_Linhas_Parquet")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .getOrCreate()
    )

    # schema de linhas (array na raiz)
    schema_item = StructType([
        StructField("cl", IntegerType(), True),
        StructField("lc", BooleanType(), True),
        StructField("lt", StringType(), True),
        StructField("sl", IntegerType(), True),
        StructField("tl", IntegerType(), True),
        StructField("tp", StringType(), True),
        StructField("ts", StringType(), True),
    ])
    schema_array = ArrayType(schema_item)

    raw = (
        spark.sparkContext
             .wholeTextFiles(f"{BRONZE_PATH.rstrip('/')}" + "/*.json")
             .toDF(["path", "raw"])
    )

    df_arr = raw.select(
        "path",
        F.from_json(F.col("raw"), schema_array).alias("arr")
    )

    df_lin = df_arr.select("path", explode(col("arr")).alias("r"))

    df_clean = (
        df_lin
        .select(
            col("r.cl").alias("line_id"),
            col("r.lt").alias("line_code"),
            col("r.sl").alias("sentido"),
            col("r.tl").alias("tipo_linha"),
            col("r.tp").alias("terminal_origem"),
            col("r.ts").alias("terminal_destino"),
        )
        .dropDuplicates(["line_id", "line_code", "sentido"])
        .withColumn("dt", to_date(current_timestamp()))
        .withColumn("ingest_ts", current_timestamp())
    )

    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(SILVER_PATH)
    )

if __name__ == "__main__":
    # 1. Inicia a sessão Spark
    spark = SparkSession.builder \
        .appName("BronzeToSilverLinhas") \
        .getOrCreate()

    # 2. Chama a função principal de transformação
    bronze_to_silver_linhas(spark)

    # 3. Encerra a sessão
    spark.stop()