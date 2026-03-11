#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/incidentes/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/incidentes_resumo_hora_v1/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/checkpoints/gold/incidentes_resumo_hora_v1/"


def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    batch_df = batch_df.filter(
        F.col("cidade").isNotNull() &
        F.col("bairro").isNotNull() &
        F.col("tipo_incidente").isNotNull() &
        F.col("dt_ref").isNotNull() &
        F.col("hora_ref").isNotNull()
    )

    if batch_df.rdd.isEmpty():
        return

    agg = (
        batch_df.groupBy(
            "estado",
            "cidade",
            "bairro",
            "tipo_incidente",
            "severidade",
            "impacto",
            "dt_ref",
            "hora_ref"
        )
        .agg(
            F.count("*").alias("qtd_incidentes"),
            F.countDistinct("incidente_id").alias("qtd_incidentes_unicos"),
            F.sum(
                F.when(F.col("faixa_interditada") == True, 1).otherwise(0)
            ).alias("qtd_faixa_interditada"),
            F.max("event_ts_ref").alias("ultimo_evento_ts")
        )
        .withColumn("gold_process_ts", F.current_timestamp())
    )

    (
        agg.write
        .mode("append")
        .format("parquet")
        .partitionBy("dt_ref", "hora_ref")
        .save(GOLD_PATH)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow_gold_incidentes_resumo_hora_v1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Silver path: {SILVER_PATH}")
    print(f"Gold path: {GOLD_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")

    silver_schema = spark.read.format("parquet").load(SILVER_PATH).schema

    df = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .option("maxFilesPerTrigger", 50)
        .load(SILVER_PATH)
    )

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
