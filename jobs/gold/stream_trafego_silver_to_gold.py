from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/trafego_v1/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/trafego_resumo_hora_v1/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/checkpoints/gold/trafego_resumo_hora_v1/"

KEY_COLS = ["cidade", "region_id", "label", "dt_ref", "hora_ref"]

spark = (
    SparkSession.builder
    .appName("urbanflow-trafego-silver-to-gold")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def aggregate_gold(df):
    return (
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

def upsert_gold(batch_df, batch_id):
    print(f"[GOLD] Iniciando batch_id={batch_id}", flush=True)

    if batch_df.rdd.isEmpty():
        print(f"[GOLD] batch_id={batch_id} vazio", flush=True)
        return

    qtd_in = batch_df.count()
    print(f"[GOLD] batch_id={batch_id} registros_entrada={qtd_in}", flush=True)

    final_df = aggregate_gold(batch_df).coalesce(1).cache()

    qtd_out = final_df.count()
    print(f"[GOLD] batch_id={batch_id} registros_agregados={qtd_out}", flush=True)

    (
        final_df.write
        .mode("append")
        .partitionBy("dt_ref", "hora_ref")
        .parquet(GOLD_PATH)
    )

    print(f"[GOLD] batch_id={batch_id} escrita concluida em {GOLD_PATH}", flush=True)

silver_schema = spark.read.parquet(SILVER_PATH).schema

silver_stream = (
    spark.readStream
    .schema(silver_schema)
    .parquet(SILVER_PATH)
)

query = (
    silver_stream.writeStream
    .foreachBatch(upsert_gold)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
