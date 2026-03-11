#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

BRONZE_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/bronze/gps/"
SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/gps_v3/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/checkpoints/gps_silver_v3/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow_silver_gps")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bronze_static = spark.read.format("parquet").load(BRONZE_PATH)
    schema = bronze_static.schema

    df = (
        spark.readStream
        .format("parquet")
        .schema(schema)
        .option("maxFilesPerTrigger", 50)
        .load(BRONZE_PATH)
    )

    df = (
        df
        .withColumn("event_ts_ref", F.coalesce(F.col("ts_evento"), F.current_timestamp()))
        .withColumn("dt_ref", F.to_date("event_ts_ref"))
        .withColumn("hora_ref", F.hour("event_ts_ref"))
        .withColumn("silver_process_ts", F.current_timestamp())
    )

    # filtro de qualidade
    df = df.filter(
        F.col("ts_evento").isNotNull() &
        (
            F.col("veiculo_id").isNotNull() |
            F.col("cidade").isNotNull()
        )
    )

    # padronização de colunas
    if "clima" in df.columns and "clima_condicao" not in df.columns:
        df = df.withColumnRenamed("clima", "clima_condicao")

    if "velocidade" in df.columns and "velocidade_kmh" not in df.columns:
        df = df.withColumnRenamed("velocidade", "velocidade_kmh")

    query = (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("dt_ref", "hora_ref")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
