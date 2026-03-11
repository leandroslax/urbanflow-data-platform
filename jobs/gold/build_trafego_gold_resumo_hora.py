from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/trafego_v1/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/trafego_resumo_hora_v1/"

KEY_COLS = ["cidade", "region_id", "label", "dt_ref", "hora_ref"]

spark = (
    SparkSession.builder
    .appName("urbanflow-build-trafego-gold-resumo-hora")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("[GOLD] Lendo dados da silver...")

df = spark.read.parquet(SILVER_PATH)

gold_df = (
    df.groupBy(*KEY_COLS)
    .agg(
        F.count("*").alias("qtd_eventos_trafego"),
        F.sum(F.when(F.col("congestion_ratio").isNotNull(), 1).otherwise(0)).alias("qtd_eventos_com_ratio"),
        F.avg("congestion_ratio").alias("congestion_ratio_medio"),
        F.max("congestion_ratio").alias("congestion_ratio_max"),
        F.min("congestion_ratio").alias("congestion_ratio_min"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("qtd_status_error")
    )
    .withColumn("gold_process_ts", F.current_timestamp())
)

print("[GOLD] Gravando no S3...")

(
    gold_df.write
    .mode("overwrite")
    .partitionBy("dt_ref", "hora_ref")
    .parquet(GOLD_PATH)
)

print("[GOLD] Reconstrução do gold concluída com sucesso.")
