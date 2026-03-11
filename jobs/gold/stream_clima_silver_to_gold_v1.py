#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/clima_v1/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/clima_resumo_hora_v1/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/checkpoints/gold_clima_resumo_hora_v1/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-clima-silver-to-gold")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_schema = spark.read.format("parquet").load(SILVER_PATH).schema

    df = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .load(SILVER_PATH)
    )

    def upsert(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        df_gold = (
            batch_df
            .filter(F.col("dt").isNotNull())
            .filter(F.col("hora").isNotNull())
            .filter(F.col("cidade").isNotNull())
            .groupBy("dt", "hora", "estado", "cidade", "condicao")
            .agg(
                F.count("*").alias("qtd_eventos_climaticos"),
                F.approx_count_distinct("fonte").alias("qtd_fontes_distintas")
            )
            .withColumn("gold_process_ts", F.current_timestamp())
        )

        (
            df_gold
            .repartition(1, "dt", "hora")
            .write
            .mode("append")
            .format("parquet")
            .partitionBy("dt", "hora")
            .save(GOLD_PATH)
        )

    query = (
        df.writeStream
        .foreachBatch(upsert)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
