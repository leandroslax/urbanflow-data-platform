#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

BRONZE_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/bronze/incidentes/"
SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/incidentes_v1/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/checkpoints/incidentes_silver_v1/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow_silver_incidentes")
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
        df.withColumn(
            "event_ts_ref",
            F.coalesce(
                F.col("ts_evento_ts"),
                F.to_timestamp(F.col("ts_evento")),
                F.to_timestamp(F.col("ts_evento"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("ts_evento"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(F.col("ts_evento"), "yyyy-MM-dd'T'HH:mm:ssX"),
                F.to_timestamp(F.col("ts_evento"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
                F.current_timestamp()
            )
        )
        .withColumn("dt_ref", F.to_date("event_ts_ref"))
        .withColumn("hora_ref", F.hour("event_ts_ref"))
        .dropDuplicates(["incidente_id", "event_ts_ref"])
        .repartition(4, "dt_ref", "hora_ref")
    )

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
