from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/clima_v1/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/clima_resumo_hora_v1/"

spark = (
    SparkSession.builder
    .appName("urbanflow-build-clima-gold-resumo-hora-v4")
    .getOrCreate()
)

print("[GOLD-CLIMA] Lendo dados da silver...")
df = spark.read.parquet(SILVER_PATH)

df_tratado = (
    df
    .withColumn("cidade_tratada", F.trim(F.col("cidade")))
    .filter(F.col("cidade_tratada").isNotNull())
    .filter(F.col("cidade_tratada") != "")
    .filter(F.upper(F.col("cidade_tratada")) != "NULL")
    .filter(F.col("dt").isNotNull())
    .filter(F.col("hora").isNotNull())
    .withColumn("condicao_climatica", F.get_json_object(F.col("condicao"), "$.condicao"))
    .withColumn("temperatura_c", F.get_json_object(F.col("condicao"), "$.temperatura_c").cast("double"))
    .withColumn("precipitacao_mm_h", F.get_json_object(F.col("condicao"), "$.chuva_mm_h").cast("double"))
    .withColumn("dt_ref", F.col("dt"))
    .withColumn("hora_ref", F.col("hora"))
)

print("[GOLD-CLIMA] Prévia tratada:")
df_tratado.select(
    "cidade_tratada",
    "condicao_climatica",
    "temperatura_c",
    "precipitacao_mm_h",
    "dt_ref",
    "hora_ref"
).show(20, truncate=False)

gold_df = (
    df_tratado
    .groupBy("cidade_tratada", "condicao_climatica", "dt_ref", "hora_ref")
    .agg(
        F.count("*").alias("qtd_eventos_clima"),
        F.avg("temperatura_c").alias("temperatura_media"),
        F.max("temperatura_c").alias("temperatura_max"),
        F.min("temperatura_c").alias("temperatura_min"),
        F.lit(None).cast("double").alias("umidade_media"),
        F.sum("precipitacao_mm_h").alias("precipitacao_total")
    )
    .withColumn("gold_process_ts", F.current_timestamp())
    .select(
        F.col("cidade_tratada").alias("cidade"),
        "condicao_climatica",
        "qtd_eventos_clima",
        "temperatura_media",
        "temperatura_max",
        "temperatura_min",
        "umidade_media",
        "precipitacao_total",
        "gold_process_ts",
        "dt_ref",
        "hora_ref"
    )
)

print("[GOLD-CLIMA] Prévia final:")
gold_df.show(20, truncate=False)

print("[GOLD-CLIMA] Gravando no S3...")
(
    gold_df.write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

print("[GOLD-CLIMA] Reconstrução concluída com sucesso.")
