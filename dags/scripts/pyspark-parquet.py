# pyspark-parquet.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_parquet_minio():
    """
    Cria um Parquet de teste e grava no MinIO.
    """
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio.local:9000")
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "miniouser")
    MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "miniopass")
    MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "false").lower() == "true"
    URL_STYLE = os.environ.get("MINIO_URL_STYLE", "path")
    BUCKET = os.environ.get("S3_BUCKET", "sptrans-test")
    PARQUET_PATH = os.environ.get("PARQUET_PATH", "trusted/test")

    spark = SparkSession.builder.appName("write_test_parquet").getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.path.style.access", "true" if URL_STYLE=="path" else "false")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true" if MINIO_USE_SSL else "false")
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # DataFrame de teste
    data = [(1, "2025-10-26", "linha-123", 12.34),
            (2, "2025-10-26", "linha-456", 9.87)]
    df = spark.createDataFrame(data, schema=["vehicle_id","data_ref","linha","vel_media"])
    df = df.withColumn("ingestion_ts", F.current_timestamp())

    dest = f"s3a://{BUCKET}/{PARQUET_PATH}/"
    df.coalesce(1).write.mode("overwrite").parquet(dest)

    print(f"Parquet escrito em: {dest}")
    spark.stop()

if __name__ == "__main__":
    create_parquet_minio()
