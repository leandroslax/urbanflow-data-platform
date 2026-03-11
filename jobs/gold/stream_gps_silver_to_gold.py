#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max,
    min,
    approx_count_distinct
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    DateType
)


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-gps-silver-to-gold")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.files.maxPartitionBytes", "67108864")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_path = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/gps_v2/"
    gold_path = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/gps_mobilidade_hora_v2/"
    checkpoint_path = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/checkpoints/gold_gps_mobilidade_hora_v2/"

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
        StructField("hora", IntegerType(), True)
    ])

    df_silver = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .option("maxFilesPerTrigger", 10)
        .load(silver_path)
    )

    df_gold = (
        df_silver
        .filter(col("dt").isNotNull())
        .filter(col("hora").isNotNull())
        .filter(col("estado").isNotNull())
        .filter(col("cidade").isNotNull())
        .filter(col("bairro").isNotNull())
        .filter(col("tipo_veiculo").isNotNull())
        .filter(col("veiculo_id").isNotNull())
        .filter(col("velocidade_kmh").isNotNull())
        .groupBy(
            "dt",
            "hora",
            "estado",
            "cidade",
            "bairro",
            "tipo_veiculo"
        )
        .agg(
            count("*").alias("qtd_sinais_gps"),
            approx_count_distinct("veiculo_id").alias("qtd_veiculos_distintos"),
            avg("velocidade_kmh").alias("velocidade_media_kmh"),
            max("velocidade_kmh").alias("velocidade_max_kmh"),
            min("velocidade_kmh").alias("velocidade_min_kmh"),
            avg("precisao_m").alias("precisao_media_m")
        )
    )

    def write_gold(batch_df, batch_id):
        (
            batch_df.write
            .mode("append")
            .format("parquet")
            .partitionBy("dt", "hora")
            .save(gold_path)
        )

    query = (
        df_gold.writeStream
        .outputMode("update")
        .foreachBatch(write_gold)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
