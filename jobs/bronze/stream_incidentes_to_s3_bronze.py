#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession, functions as F, types as T


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


def incidentes_schema() -> T.StructType:
    return T.StructType([
        T.StructField("incidente_id", T.StringType(), True),
        T.StructField("ts_evento", T.StringType(), True),
        T.StructField("tipo_incidente", T.StringType(), True),
        T.StructField("severidade", T.StringType(), True),
        T.StructField("estado", T.StringType(), True),
        T.StructField("cidade", T.StringType(), True),
        T.StructField("bairro", T.StringType(), True),
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("faixa_interditada", T.BooleanType(), True),
        T.StructField("impacto", T.StringType(), True),
        T.StructField("fonte", T.StringType(), True),
    ])


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--bootstrap", required=True, help="MSK bootstrap")
    ap.add_argument("--client-props", required=False, help="mantido por compatibilidade (não é usado diretamente no Spark)")
    ap.add_argument("--topic", default="urbanflow-incidentes-bruto")

    ap.add_argument("--s3-base", default="s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow")
    ap.add_argument("--output-prefix", default="bronze/incidentes")
    ap.add_argument("--checkpoint-prefix", default="checkpoints/bronze/incidentes")

    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--max-offsets-per-trigger", type=int, default=20000)
    ap.add_argument("--trigger-seconds", type=int, default=10)

    args = ap.parse_args()

    spark = build_spark("urbanflow-bronze-incidentes")

    schema = incidentes_schema()
    kopts = kafka_iam_options(args.bootstrap)

    raw = (
        spark.readStream.format("kafka")
        .options(**kopts)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )

    df = (
        raw.select(F.col("value").cast("string").alias("json_str"))
        .select(F.from_json(F.col("json_str"), schema).alias("r"))
        .select("r.*")
    )

    df = df.withColumn(
        "ts_evento_ts",
        F.to_timestamp(F.regexp_replace(F.col("ts_evento"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss")
    )

    df = df.withColumn("dt", F.to_date(F.col("ts_evento_ts")))

    output_path = f"{args.s3_base.rstrip('/')}/{args.output_prefix.strip('/')}"
    checkpoint_path = f"{args.s3_base.rstrip('/')}/{args.checkpoint_prefix.strip('/')}"

    query = (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("dt")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("urbanflow-bronze-incidentes")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
