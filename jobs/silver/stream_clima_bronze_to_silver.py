#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, current_timestamp, get_json_object, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    TimestampType, DateType
)

BUCKET = "s3a://urbanflow-datalake-dev-us-east-1-139961319000"

BRONZE_PATH = f"{BUCKET}/urbanflow/bronze/clima/"
SILVER_PATH = f"{BUCKET}/urbanflow/silver/clima_v1/"
CHECKPOINT_PATH = f"{BUCKET}/checkpoints/clima_silver_v1/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-clima-bronze-to-silver")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("raw_json", StringType(), True),
        StructField("kafka_ts", TimestampType(), True),
        StructField("ts_evento", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("cidade", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("fonte", StringType(), True),
        StructField("clima", StringType(), True),
        StructField("ts_evento_ts", TimestampType(), True),
        StructField("dt", DateType(), True)
    ])

    df_bronze = (
        spark.readStream
        .format("parquet")
        .schema(schema)
        .option("maxFilesPerTrigger", 10)
        .load(BRONZE_PATH)
    )

    df_silver = (
        df_bronze
        .withColumn(
            "condicao",
            get_json_object(col("raw_json"), "$.clima")
        )
        .withColumn(
            "ts_evento_ref",
            col("ts_evento_ts")
        )
        .filter(col("ts_evento_ref").isNotNull())
        .filter(col("estado").isNotNull())
        .filter(col("cidade").isNotNull())
        .withColumn("hora", hour(col("ts_evento_ref")))
        .withColumn("silver_process_ts", current_timestamp())
        .select(
            col("ts_evento_ref").alias("ts_evento"),
            col("dt"),
            col("hora"),
            col("estado"),
            col("cidade"),
            col("latitude"),
            col("longitude"),
            col("fonte"),
            col("condicao"),
            col("silver_process_ts")
        )
    )

    query = (
        df_silver.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("dt", "hora")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
