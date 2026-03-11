#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/viagens_v2/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/viagens_resumo_hora_v3/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/checkpoints/gold/viagens_resumo_hora_v3/"


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    gold_df = (
        batch_df
        .groupBy("status", "fonte", "ano", "mes", "dia", "hora")
        .agg(
            F.count("*").alias("qtd_registros"),
            F.countDistinct("id_viagem").alias("qtd_viagens_distintas"),
            F.countDistinct("id_passageiro").alias("qtd_passageiros_distintos"),
            F.countDistinct("id_motorista").alias("qtd_motoristas_distintos"),
            F.avg("preco").alias("preco_medio"),
            F.sum("preco").alias("preco_total")
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
        .partitionBy("ano", "mes", "dia")
        .parquet(GOLD_PATH)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow_gold_viagens_v3")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
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
