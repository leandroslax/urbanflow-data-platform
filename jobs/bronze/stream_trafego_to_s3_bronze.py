#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UrbanFlow - Bronze Tráfego (TomTom -> Kafka/MSK IAM -> S3 Parquet)

- Lê do tópico Kafka (MSK IAM) via Structured Streaming
- Persiste BRONZE "raw + metadados" e particiona por dt
- dt é derivado de ts_evento do payload (ISO UTC)

Execução via systemd normalmente passa:
  --bootstrap <host:port>
  --client-props <arquivo .properties do IAM>
"""

import argparse
import os
from typing import Dict

from pyspark.sql import SparkSession, functions as F


def read_java_properties(path: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    if not path:
        return props
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" in s:
                k, v = s.split("=", 1)
                props[k.strip()] = v.strip()
    return props


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanFlow Bronze Tráfego (Kafka -> S3)")
    ap.add_argument("--bootstrap", required=True, help="MSK bootstrap servers host:port")
    ap.add_argument("--topic", default="urbanflow-trafego-bruto", help="Nome do tópico Kafka")
    ap.add_argument("--client-props", default="", help="Arquivo .properties com configs de auth/SSL (consumer)")

    # S3 paths (mantém padrão do seu projeto)
    ap.add_argument("--s3-base", default="s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow",
                    help="Base S3 (s3a://.../urbanflow)")
    ap.add_argument("--output-prefix", default="bronze/trafego", help="Prefixo dentro do s3-base p/ output")
    ap.add_argument("--checkpoint-prefix", default="checkpoints/bronze/trafego",
                    help="Prefixo dentro do s3-base p/ checkpoint")

    # Streaming
    ap.add_argument("--startingOffsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--failOnDataLoss", default="false", choices=["true", "false"])
    ap.add_argument("--trigger-seconds", type=int, default=10)

    args = ap.parse_args()

    spark = build_spark("urbanflow-bronze-trafego")

    # consumer props (IAM, TLS, etc)
    props = read_java_properties(args.client_props)
    # garantimos bootstrap aqui (mesmo que venha no .properties)
    props["kafka.bootstrap.servers"] = args.bootstrap

    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.startingOffsets)
        .option("failOnDataLoss", args.failOnDataLoss)
    )
    for k, v in props.items():
        # evita sobrescrever opções já setadas acima, mas permite auth
        if k in ["kafka.bootstrap.servers"]:
            continue
        reader = reader.option(k, v)

    raw = reader.load()

    bronze = (
        raw.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ts_evento", F.get_json_object(F.col("value"), "$.ts_evento"))
        .withColumn(
            "ts_evento_ts",
            F.to_timestamp(F.regexp_replace(F.col("ts_evento"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .withColumn("dt", F.to_date(F.col("ts_evento_ts")))
    )

    output_path = f"{args.s3_base.rstrip('/')}/{args.output_prefix.strip('/')}"
    checkpoint_path = f"{args.s3_base.rstrip('/')}/{args.checkpoint_prefix.strip('/')}"

    query = (
        bronze.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("dt")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("urbanflow-bronze-trafego")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
