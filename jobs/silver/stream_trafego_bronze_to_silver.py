#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, get_json_object, to_timestamp, to_date, hour
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, IntegerType, LongType, DateType
)

BUCKET = "s3a://urbanflow-datalake-dev-us-east-1-139961319000"

BRONZE_PATH = f"{BUCKET}/urbanflow/bronze/trafego/"
SILVER_PATH = f"{BUCKET}/urbanflow/silver/trafego_v1/"
CHECKPOINT_PATH = f"{BUCKET}/checkpoints/trafego_silver_v1/"


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-trafego-bronze-to-silver")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bronze_schema = StructType([
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("ingest_ts", TimestampType(), True),
        StructField("ts_evento", StringType(), True),
        StructField("ts_evento_clean", StringType(), True),
        StructField("ts_evento_ts", TimestampType(), True),
        StructField("dt", DateType(), True),
    ])

    df_bronze = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .option("maxFilesPerTrigger", 10)
        .load(BRONZE_PATH)
    )

    df_silver = (
        df_bronze
        .withColumn("event_id", get_json_object(col("value"), "$.event_id"))
        .withColumn("tipo", get_json_object(col("value"), "$.tipo"))
        .withColumn("provider", get_json_object(col("value"), "$.provider"))
        .withColumn("cidade", get_json_object(col("value"), "$.city"))
        .withColumn("region_id", get_json_object(col("value"), "$.region_id"))
        .withColumn("label", get_json_object(col("value"), "$.label"))
        .withColumn("point_lat", get_json_object(col("value"), "$.point_lat").cast("double"))
        .withColumn("point_lon", get_json_object(col("value"), "$.point_lon").cast("double"))
        .withColumn("ts_evento_payload", get_json_object(col("value"), "$.ts_evento"))
        .withColumn("ts_coleta", to_timestamp(get_json_object(col("value"), "$.ts_coleta")))
        .withColumn("status", get_json_object(col("value"), "$.status"))
        .withColumn("error_message", get_json_object(col("value"), "$.error_message"))
        .withColumn("congestion_ratio", get_json_object(col("value"), "$.congestion_ratio").cast("double"))
        .withColumn("ts_evento_ref", col("ts_evento_ts"))
        .withColumn("dt_ref", to_date(col("ts_evento_ref")))
        .withColumn("hora_ref", hour(col("ts_evento_ref")))
        .withColumn("silver_process_ts", current_timestamp())
        .filter(col("ts_evento_ref").isNotNull())
        .filter(col("cidade").isNotNull())
        .select(
            "event_id",
            "tipo",
            "provider",
            "cidade",
            "region_id",
            "label",
            "point_lat",
            "point_lon",
            "ts_evento_payload",
            "ts_evento_ref",
            "ts_coleta",
            "status",
            "error_message",
            "congestion_ratio",
            "kafka_timestamp",
            "ingest_ts",
            "topic",
            "partition",
            "offset",
            "silver_process_ts",
            "dt_ref",
            "hora_ref"
        )
        .dropDuplicates(["event_id"])
    )

    query = (
        df_silver.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("dt_ref", "hora_ref")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
