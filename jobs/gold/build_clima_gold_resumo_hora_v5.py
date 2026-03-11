from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/clima_v1/"
GOLD_TMP_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold_tmp/clima_resumo_hora_v1/"
GOLD_FINAL_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/clima_resumo_hora_v1/"

spark = (
    SparkSession.builder
    .appName("urbanflow-build-clima-gold-resumo-hora-v5")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
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

print("[GOLD-CLIMA] Gravando em path temporário...")
(
    gold_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .partitionBy("dt_ref", "hora_ref")
    .parquet(GOLD_TMP_PATH)
)

print("[GOLD-CLIMA] Escrita temporária concluída com sucesso.")
print(f"[GOLD-CLIMA] TMP_PATH={GOLD_TMP_PATH}")
print(f"[GOLD-CLIMA] FINAL_PATH={GOLD_FINAL_PATH}")
