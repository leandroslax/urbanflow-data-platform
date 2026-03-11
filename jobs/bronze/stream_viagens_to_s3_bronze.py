#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UrbanFlow - Bronze Viagens (Kafka/MSK IAM -> S3 Parquet)

- Lê do tópico Kafka (MSK IAM) via Structured Streaming
- Persiste BRONZE "raw + metadados" e particiona por dt
- dt é derivado de ts_evento do payload (ISO UTC), para permitir janela de 30 dias realista

Execução via systemd normalmente passa:
  --bootstrap <host:port>
  --client-props <arquivo .properties do IAM>
"""

import argparse
import os
from typing import Dict

from pyspark.sql import SparkSession, functions as F


def read_java_properties(path: str) -> Dict[str, str]:
    """
    Lê um arquivo .properties (key=value) e retorna dict.
    Ignora linhas vazias e comentários (# ou ;).
    """
    props: Dict[str, str] = {}
    if not path:
        return props
    if not os.path.exists(path):
        raise FileNotFoundError(f"client-props não encontrado: {path}")

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            props[k.strip()] = v.strip()
    return props


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="UrbanFlow Bronze Viagens (Kafka -> S3)")
    ap.add_argument("--bootstrap", required=True, help="MSK bootstrap servers host:port")
    ap.add_argument("--topic", default="urbanflow-viagens-bruto", help="Nome do tópico Kafka")
    ap.add_argument("--client-props", default="", help="Arquivo .properties com configs de auth/SSL (producer/consumer)")

    # S3 paths (mantém padrão do seu projeto)
    ap.add_argument("--s3-base", default="s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow",
                    help="Base S3 (s3a://.../urbanflow)")
    ap.add_argument("--output-prefix", default="bronze/viagens", help="Prefixo dentro do s3-base p/ output")
    ap.add_argument("--checkpoint-prefix", default="checkpoints/bronze/viagens",
                    help="Prefixo dentro do s3-base p/ checkpoint")

    # Streaming options
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                    help="Offsets iniciais (use earliest só se for reprocessar histórico e limpar checkpoint)")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=0,
                    help="Limite de offsets por micro-batch (0 = desabilitado)")
    ap.add_argument("--trigger-seconds", type=int, default=30, help="Intervalo do micro-batch em segundos")
    ap.add_argument("--log-level", default="INFO", help="Nível de log do Spark (INFO/WARN/ERROR)")

    args = ap.parse_args()

    spark = build_spark("urbanflow-bronze-viagens")
    spark.sparkContext.setLogLevel(args.log_level)

    client_props = read_java_properties(args.client_props)

    # Base reader Kafka
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
    )

    # aplica configs do client-props como kafka.* quando necessário
    # Ex.: security.protocol, sasl.mechanism, sasl.jaas.config, sasl.client.callback.handler.class, etc.
    for k, v in client_props.items():
        # se o arquivo já vier com "kafka." no começo, respeita; senão prefixa com "kafka."
        if k.startswith("kafka."):
            reader = reader.option(k, v)
        else:
            reader = reader.option(f"kafka.{k}", v)

    if args.max_offsets_per_trigger and args.max_offsets_per_trigger > 0:
        reader = reader.option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))

    raw = reader.load()

    # -----------------------------
    # BRONZE: payload + metadados
    # -----------------------------
    # Extrai ts_evento (ISO UTC), converte p/ timestamp e cria dt pela data do evento.
    # Ex ts_evento: "2026-03-04T18:10:00Z"
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
        # Spark entende ISO 8601 com 'Z' em muitos casos, mas pra robustez removemos 'Z' e informamos o pattern.
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
        .queryName("urbanflow-bronze-viagens")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
