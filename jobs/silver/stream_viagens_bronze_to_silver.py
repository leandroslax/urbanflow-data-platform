#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.column import Column

BRONZE_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/bronze/viagens/"
SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/viagens_v2/"
CHECKPOINT_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/checkpoints/silver/viagens_v2/"


def col_if_exists(df, name: str, cast_type: str = None) -> Column:
    if name in df.columns:
        c = F.col(name)
        return c.cast(cast_type) if cast_type else c
    return F.lit(None).cast(cast_type) if cast_type else F.lit(None)


def json_field(name: str) -> Column:
    return F.get_json_object(F.col("value"), f"$.{name}")


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow_silver_viagens_v2")
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

    # Extrai campos de negócio do JSON em `value`
    status_col = F.coalesce(
        col_if_exists(df, "status", "string"),
        json_field("status"),
        F.lit("desconhecido")
    )

    fonte_col = F.coalesce(
        col_if_exists(df, "fonte", "string"),
        json_field("fonte"),
        F.lit("simulado")
    )

    id_viagem_col = F.coalesce(
        col_if_exists(df, "id_viagem", "string"),
        col_if_exists(df, "viagem_id", "string"),
        json_field("id_viagem"),
        json_field("viagem_id")
    )

    id_passageiro_col = F.coalesce(
        col_if_exists(df, "id_passageiro", "string"),
        col_if_exists(df, "passageiro_id", "string"),
        json_field("id_passageiro"),
        json_field("passageiro_id")
    )

    id_motorista_col = F.coalesce(
        col_if_exists(df, "id_motorista", "string"),
        col_if_exists(df, "motorista_id", "string"),
        json_field("id_motorista"),
        json_field("motorista_id")
    )

    preco_col = F.coalesce(
        col_if_exists(df, "preco", "double"),
        col_if_exists(df, "preco_estimado", "double"),
        json_field("preco").cast("double"),
        json_field("preco_estimado").cast("double")
    )

    event_ts_col = F.coalesce(
        col_if_exists(df, "event_ts", "timestamp"),
        col_if_exists(df, "ts_evento_ts", "timestamp"),
        F.to_timestamp(col_if_exists(df, "ts_evento", "string")),
        F.to_timestamp(json_field("ts_evento")),
        col_if_exists(df, "kafka_timestamp", "timestamp"),
        col_if_exists(df, "ingest_ts", "timestamp"),
        F.current_timestamp()
    )

    ingest_ts_col = F.coalesce(
        col_if_exists(df, "ingest_ts", "timestamp"),
        F.current_timestamp()
    )

    silver_df = (
        df
        .withColumn("status", status_col)
        .withColumn("fonte", fonte_col)
        .withColumn("id_viagem", id_viagem_col)
        .withColumn("id_passageiro", id_passageiro_col)
        .withColumn("id_motorista", id_motorista_col)
        .withColumn("preco", preco_col)
        .withColumn("event_ts", event_ts_col)
        .withColumn("ingest_ts", ingest_ts_col)
        .withColumn("ano", F.year("event_ts"))
        .withColumn("mes", F.month("event_ts"))
        .withColumn("dia", F.dayofmonth("event_ts"))
        .withColumn("hora", F.hour("event_ts"))
        .select(
            "status",
            "fonte",
            "event_ts",
            "ingest_ts",
            "id_viagem",
            "id_passageiro",
            "id_motorista",
            "preco",
            "ano",
            "mes",
            "dia",
            "hora"
        )
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("id_viagem").isNotNull())
        .dropDuplicates(["id_viagem", "event_ts"])
        .repartition(4, "ano", "mes", "dia")
    )

    query = (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ano", "mes", "dia")
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
