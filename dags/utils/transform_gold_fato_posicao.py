from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from delta.tables import DeltaTable


spark = (
    SparkSession.builder.appName("SilverToGold_fato_posicao")
    .getOrCreate()
)
print("✅ SparkSession inicializada")

# Paths
today = "data_ref=" + datetime.today().strftime('%Y-%m-%d')
SILVER_POSICAO = "s3a://silver/posicao/"
GOLD_DIM_LINHA = "s3a://gold/dim_linha/"
GOLD_FATO_POSICAO = "s3a://gold/fato_posicao/"

print(f"📂 Lendo Silver Posicao: {SILVER_POSICAO}")
print(f"📂 Lendo Dim Linha: {GOLD_DIM_LINHA}")

# Leitura
posicao = spark.read.format("delta").load(SILVER_POSICAO)
dim_linha = spark.read.format("delta").load(GOLD_DIM_LINHA)

# Join com dim linha pra manter linhas que existem na dimensão
fato = (
    posicao.alias("p")
    .join(dim_linha.alias("l"), "codigo_linha", "inner")
    .select(
        F.col("p.codigo_linha"),
        F.col("p.codigo_veiculo"),
        F.col("p.sentido"),
        F.col("p.acessibilidade"),
        F.col("p.latitude"),
        F.col("p.longitude"),
        F.col("p.hora_referencia"),
        F.col("p.ultima_atualizacao"),
        F.col("p.data_ref"),
        F.current_timestamp().alias("transform_gold_timestamp")
    )
)

# Salvar 
if DeltaTable.isDeltaTable(spark, GOLD_FATO_POSICAO):
    fato_table = DeltaTable.forPath(spark, GOLD_FATO_POSICAO)
    (
        fato_table.alias("tgt")
        .merge(
            fato.alias("src"),
            "tgt.codigo_veiculo = src.codigo_veiculo AND tgt.hora_referencia = src.hora_referencia"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    fato.write.format("delta").mode("overwrite").partitionBy("data_ref").save(GOLD_FATO_POSICAO)

print(f"fato_posicao salvo em {GOLD_FATO_POSICAO}")
spark.stop()
print("SparkSession encerrada.")