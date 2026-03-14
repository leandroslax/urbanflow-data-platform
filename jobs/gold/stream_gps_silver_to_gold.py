#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/gps_v2/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/gps_mobilidade_hora_v3/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/checkpoints/gold_gps_mobilidade_hora_v3/"

silver_schema = StructType([
    StructField("ts_evento", TimestampType(), True),
    StructField("veiculo_id", StringType(), True),
    StructField("tipo_veiculo", StringType(), True),
    StructField("linha", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("velocidade_kmh", DoubleType(), True),
    StructField("rumo_graus", IntegerType(), True),
    StructField("precisao_m", IntegerType(), True),
    StructField("fonte_gps", StringType(), True),
    StructField("status_sinal", StringType(), True),
    StructField("clima_condicao", StringType(), True),
    StructField("clima_temperatura_c", DoubleType(), True),
    StructField("clima_chuva_mm_h", DoubleType(), True),
    StructField("clima_visibilidade_m", IntegerType(), True),
    StructField("silver_process_ts", TimestampType(), True),
    StructField("dt", DateType(), True),
    StructField("hora", IntegerType(), True),
])

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-gps-silver-to-gold-v3")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=======================================")
    print("UrbanFlow Gold GPS Mobilidade Hora v3")
    print(f"Silver: {SILVER_PATH}")
    print(f"Gold: {GOLD_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")
    print("=======================================")

    df = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .option("maxFilesPerTrigger", 10)
        .load(SILVER_PATH)
    )

    df = (
        df.filter(F.col("dt").isNotNull())
          .filter(F.col("hora").isNotNull())
          .filter(F.col("estado").isNotNull())
          .filter(F.col("cidade").isNotNull())
          .filter(F.col("bairro").isNotNull())
          .filter(F.col("tipo_veiculo").isNotNull())
          .filter(F.col("veiculo_id").isNotNull())
          .filter(F.col("velocidade_kmh").isNotNull())
    )

    def upsert_gold(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        df_gold = (
            batch_df
            .groupBy("dt", "hora", "estado", "cidade", "bairro", "tipo_veiculo")
            .agg(
                F.count("*").alias("qtd_sinais_gps"),
                F.approx_count_distinct("veiculo_id").alias("qtd_veiculos_distintos"),
                F.avg("velocidade_kmh").alias("velocidade_media_kmh"),
                F.max("velocidade_kmh").alias("velocidade_max_kmh"),
                F.min("velocidade_kmh").alias("velocidade_min_kmh"),
                F.avg("precisao_m").alias("precisao_media_m")
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
        .foreachBatch(upsert_gold)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
