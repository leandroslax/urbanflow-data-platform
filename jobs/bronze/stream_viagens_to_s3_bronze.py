#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def kafka_iam_options(bootstrap: str) -> dict:
    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--client-props", required=False, help="mantido por compatibilidade")
    ap.add_argument("--topic", default="urbanflow-viagens-bruto")

    ap.add_argument("--s3-base", default="s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow")
    ap.add_argument("--output-prefix", default="bronze/viagens")
    ap.add_argument("--checkpoint-prefix", default="checkpoints/bronze/viagens")

    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--max-offsets-per-trigger", type=int, default=20000)
    ap.add_argument("--trigger-seconds", type=int, default=10)

    args = ap.parse_args()

    spark = build_spark("urbanflow-bronze-viagens")
    kopts = kafka_iam_options(args.bootstrap)

    raw = (
        spark.readStream.format("kafka")
        .options(**kopts)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )

    # Bronze: guarda o evento bruto (sem schema) + metadados
    df = raw.select(
        F.col("timestamp").alias("kafka_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("value").cast("string").alias("json")
    )

    # partição por dia baseada no timestamp do Kafka (sempre existe)
    df = df.withColumn("dt", F.date_format(F.col("kafka_ts"), "yyyy-MM-dd"))

    output_path = f"{args.s3_base.rstrip('/')}/{args.output_prefix.strip('/')}"
    checkpoint_path = f"{args.s3_base.rstrip('/')}/{args.checkpoint_prefix.strip('/')}"

    (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("dt")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("urbanflow-bronze-viagens")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()