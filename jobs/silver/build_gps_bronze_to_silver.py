#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

BRONZE_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/bronze/gps/"
SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/gps_v4/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-build-gps-bronze-to-silver-v4")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[SILVER-GPS-V4] Lendo bronze...")
    df = spark.read.parquet(BRONZE_PATH)

    cols = df.columns

    if "ts_evento" in cols:
        df = df.withColumn("event_ts_ref", F.to_timestamp("ts_evento"))
    else:
        df = df.withColumn("event_ts_ref", F.current_timestamp())

    df = (
        df.withColumn("dt_ref", F.to_date("event_ts_ref"))
          .withColumn("hora_ref", F.hour("event_ts_ref"))
    )

    if "cidade" not in cols:
        df = df.withColumn("cidade", F.lit(None).cast("string"))

    if "veiculo_id" not in cols:
        df = df.withColumn("veiculo_id", F.lit(None).cast("string"))

    df = df.filter(
        F.col("ts_evento").isNotNull() &
        (
            F.col("veiculo_id").isNotNull() |
            F.col("cidade").isNotNull()
        )
    )

    print("[SILVER-GPS-V4] Gravando silver...")
    (
        df.write
        .mode("overwrite")
        .partitionBy("dt_ref", "hora_ref")
        .parquet(SILVER_PATH)
    )

    print("[SILVER-GPS-V4] Reconstrução concluída com sucesso.")

if __name__ == "__main__":
    main()
