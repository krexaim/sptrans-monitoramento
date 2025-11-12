from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------
# Inicializa Spark
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Validate_Fato_Posicao")
    .getOrCreate()
)
print("✅ SparkSession inicializada")

# Caminhos
GOLD_FATO_POSICAO = "s3a://gold/fato_posicao/"
MONITORING_OUTPUT = "s3a://gold/monitoring/fato_posicao_daily.csv"

print(f"📂 Lendo: {GOLD_FATO_POSICAO}")

# ---------------------------------------------------------
# Leitura da tabela fato
# ---------------------------------------------------------
try:
    fato = spark.read.format("delta").load(GOLD_FATO_POSICAO)
except Exception as e:
    print(f"❌ Erro ao ler fato_posicao: {e}")
    spark.stop()
    exit(1)

# ---------------------------------------------------------
# Agregação por data_ref
# ---------------------------------------------------------
resumo = (
    fato.groupBy("data_ref")
        .agg(
            F.count("*").alias("total_registros"),
            F.countDistinct("codigo_veiculo").alias("onibus_unicos"),
            F.countDistinct("codigo_linha").alias("linhas_ativas"),
            F.min("hora_referencia").alias("inicio_dia"),
            F.max("hora_referencia").alias("fim_dia")
        )
        .orderBy("data_ref", ascending=False)
)

print("\n📊 Resumo histórico fato_posicao:")
resumo.show(truncate=False)

# ---------------------------------------------------------
# Regras simples de qualidade
# ---------------------------------------------------------
min_registros = 100  # limiar mínimo para considerar o dia completo
dias_incompletos = resumo.filter(F.col("total_registros") < min_registros)

if dias_incompletos.count() > 0:
    print("⚠️ Dias com baixa quantidade de registros detectados:")
    dias_incompletos.show(truncate=False)
else:
    print("✅ Todos os dias têm volume satisfatório.")

# ---------------------------------------------------------
# Exportar resumo diário em CSV (para Metabase / monitoramento)
# ---------------------------------------------------------
try:
    (
        resumo.coalesce(1)  # gera um único arquivo CSV
        .write.mode("overwrite")
        .option("header", True)
        .csv(MONITORING_OUTPUT)
    )
    print(f"💾 Resumo diário exportado para: {MONITORING_OUTPUT}")
except Exception as e:
    print(f"⚠️ Erro ao exportar CSV: {e}")

spark.stop()
print("🏁 Validação concluída com sucesso.")
