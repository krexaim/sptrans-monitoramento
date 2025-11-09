# utils/bronze_to_silver.py

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, explode, current_timestamp, to_date
)
from pyspark.sql import functions as F

def bronze_to_silver_paradas():
    today = datetime.now().strftime("%Y/%m/%d")
    BRONZE_PATH = f"s3a://bronze/paradas/{today}/"
    SILVER_PATH = "s3a://silver/linha_parada/"

    spark = (
        SparkSession.builder.appName("BronzeToSilver_Paradas_Parquet")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .getOrCreate()
    )

    stop_struct = StructType([
        StructField("cp", LongType(),   True),
        StructField("np", StringType(), True),
        StructField("ed", StringType(), True),
        StructField("py", DoubleType(), True),
        StructField("px", DoubleType(), True),
    ])
    schema_map = MapType(StringType(), ArrayType(stop_struct), True)

    raw = (
        spark.sparkContext
             .wholeTextFiles(f"{BRONZE_PATH.rstrip('/')}" + "/*.json")
             .toDF(["path", "raw"])
    )

    df_parsed = raw.select(
        "path",
        F.from_json(F.col("raw"), schema_map).alias("stops_map")
    )

    df_line_array = df_parsed.select(
        "path",
        explode(col("stops_map")).alias("cl_str", "stops_array")
    )

    df_stops = df_line_array.select(
        "path",
        col("cl_str").cast("int").alias("line_id"),
        explode(col("stops_array")).alias("stop")
    )

    df_clean = (
        df_stops
        .select(
            col("line_id"),
            col("stop.cp").alias("stop_id"),
            col("stop.np").alias("stop_name"),
            col("stop.ed").alias("endereco"),
            col("stop.py").alias("latitude"),
            col("stop.px").alias("longitude"),
        )
        .dropDuplicates(["line_id", "stop_id"])
        .withColumn("dt", to_date(current_timestamp()))
        .withColumn("ingest_ts", current_timestamp())
    )

    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("dt", "line_id")
        .parquet(SILVER_PATH)
    )

if __name__ == "__main__":
    # 1. Inicia a sessão Spark
    spark = SparkSession.builder \
        .appName("BronzeToSilverParadas") \
        .getOrCreate()

    # 2. Chama a função principal de transformação
    bronze_to_silver_paradas(spark)

    # 3. Encerra a sessão
    spark.stop()
