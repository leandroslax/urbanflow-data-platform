from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/incidentes/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/incidentes_resumo_hora_v1/"

spark = (
    SparkSession.builder
    .appName("urbanflow-build-incidentes-gold-resumo-hora-v3")
    .getOrCreate()
)

print("[GOLD-INCIDENTES] Lendo dados da silver...")
df = spark.read.parquet(SILVER_PATH)

print("[GOLD-INCIDENTES] Schema:")
df.printSchema()

print("[GOLD-INCIDENTES] Amostra original:")
df.select("incidente_id", "cidade", "tipo_incidente", "severidade", "dt_ref", "hora_ref").show(20, truncate=False)

df_tratado = (
    df
    .filter(F.col("cidade").isNotNull())
    .filter(F.trim(F.col("cidade")) != "")
    .filter(F.upper(F.trim(F.col("cidade"))) != "NULL")
    .filter(F.col("dt_ref").isNotNull())
    .filter(F.col("hora_ref").isNotNull())
    .withColumn("tipo", F.coalesce(F.col("tipo_incidente"), F.lit("nao_informado")))
    .withColumn("severidade_tratada", F.coalesce(F.col("severidade"), F.lit("nao_informado")))
)

print("[GOLD-INCIDENTES] Amostra tratada:")
df_tratado.select(
    "incidente_id",
    "cidade",
    "tipo",
    "severidade_tratada",
    "dt_ref",
    "hora_ref"
).show(20, truncate=False)

gold_df = (
    df_tratado
    .groupBy("cidade", "tipo", "severidade_tratada", "dt_ref", "hora_ref")
    .agg(
        F.count("*").alias("qtd_incidentes"),
        F.countDistinct("incidente_id").alias("qtd_incidentes_distintos")
    )
    .withColumn("gold_process_ts", F.current_timestamp())
    .select(
        "cidade",
        "tipo",
        F.col("severidade_tratada").alias("severidade"),
        "qtd_incidentes",
        "qtd_incidentes_distintos",
        "gold_process_ts",
        "dt_ref",
        "hora_ref"
    )
)

print("[GOLD-INCIDENTES] Prévia final:")
gold_df.show(20, truncate=False)

print("[GOLD-INCIDENTES] Gravando no S3...")
(
    gold_df.write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

print("[GOLD-INCIDENTES] Reconstrução concluída com sucesso.")
