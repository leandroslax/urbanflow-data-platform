#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import *


# ----------------------------------------------------------
# Args
# ----------------------------------------------------------

ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", required=True)
ap.add_argument("--client-props", required=True)

args = ap.parse_args()

bootstrap = args.bootstrap


# ----------------------------------------------------------
# Spark
# ----------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("urbanflow-bronze-gps")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ----------------------------------------------------------
# Schema GPS
# ----------------------------------------------------------

schema = StructType([

    StructField("ts_evento", StringType()),

    StructField("veiculo_id", StringType()),
    StructField("tipo_veiculo", StringType()),
    StructField("linha", StringType()),

    StructField("estado", StringType()),
    StructField("cidade", StringType()),
    StructField("bairro", StringType()),

    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),

    StructField("velocidade_kmh", DoubleType()),
    StructField("rumo_graus", IntegerType()),
    StructField("precisao_m", IntegerType()),

    StructField("fonte_gps", StringType()),
    StructField("status_sinal", StringType()),

    StructField("clima", StructType([
        StructField("condicao", StringType()),
        StructField("temperatura_c", DoubleType()),
        StructField("chuva_mm_h", DoubleType()),
        StructField("visibilidade_m", IntegerType())
    ]))

])


# ----------------------------------------------------------
# Kafka Read
# ----------------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", "urbanflow-gps-bruto")

    # MSK IAM
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
    .option(
        "kafka.sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required;"
    )
    .option(
        "kafka.sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    )

    .option("startingOffsets", "latest")
    .load()
)


# ----------------------------------------------------------
# JSON parse
# ----------------------------------------------------------

json_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)


# ----------------------------------------------------------
# Timestamp + Partição
# ----------------------------------------------------------

df = (
    json_df
    .withColumn("ts_evento", to_timestamp("ts_evento"))
    .withColumn("dt", to_date("ts_evento"))
)


# ----------------------------------------------------------
# S3 paths
# ----------------------------------------------------------

output_path = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/bronze/gps"

checkpoint_path = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/checkpoints/bronze/gps"


# ----------------------------------------------------------
# Write Stream
# ----------------------------------------------------------

query = (
    df.writeStream
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)

    .partitionBy("dt")

    .outputMode("append")

    .trigger(processingTime="30 seconds")

    .start()
)


# ----------------------------------------------------------
# Await
# ----------------------------------------------------------

query.awaitTermination()