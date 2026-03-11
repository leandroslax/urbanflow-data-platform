#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/viagens/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/viagens_resumo_hora_v1/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/checkpoints/gold/viagens_resumo_hora_v2/"


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession

    gold_df = (
        batch_df
        .withColumn("event_ts_ref", F.coalesce(F.col("event_ts"), F.col("ts_evento_ts"), F.current_timestamp()))
        .withColumn("ano", F.year("event_ts_ref"))
        .withColumn("mes", F.month("event_ts_ref"))
        .withColumn("dia", F.dayofmonth("event_ts_ref"))
        .withColumn("hora", F.hour("event_ts_ref"))
        .groupBy("status", "fonte", "ano", "mes", "dia", "hora")
        .agg(
            F.count("*").alias("qtd_registros"),
            F.countDistinct("viagem_id").alias("qtd_viagens_distintas"),
            F.countDistinct("passageiro_id").alias("qtd_passageiros_distintos"),
            F.countDistinct("motorista_id").alias("qtd_motoristas_distintos"),
            F.avg("preco_estimado").alias("preco_medio"),
            F.sum("preco_estimado").alias("preco_total")
        )
        .withColumn("gold_process_ts", F.current_timestamp())
        .select(
            "status",
            "fonte",
            "qtd_registros",
            "qtd_viagens_distintas",
            "qtd_passageiros_distintos",
            "qtd_motoristas_distintos",
            "preco_medio",
            "preco_total",
            "gold_process_ts",
            "ano",
            "mes",
            "dia",
            "hora"
        )
    )

    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("ano", "mes", "dia", "hora")
        .parquet(GOLD_PATH)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-gold-viagens-resumo-hora-v2")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_schema = spark.read.parquet(SILVER_PATH).schema

    silver_stream = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .load(SILVER_PATH)
    )

    query = (
        silver_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
