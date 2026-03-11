#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F, types as T
import sys
import traceback

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/viagens/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/viagens_resumo_hora_v2/"


def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-rebuild-viagens-gold-v2")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"SILVER_PATH={SILVER_PATH}")
    print(f"GOLD_PATH={GOLD_PATH}")

    schema = T.StructType([
        T.StructField("status", T.StringType(), True),
        T.StructField("fonte", T.StringType(), True),
        T.StructField("event_ts", T.TimestampType(), True),
        T.StructField("ingest_ts", T.TimestampType(), True),
        T.StructField("id_viagem", T.StringType(), True),
        T.StructField("id_passageiro", T.StringType(), True),
        T.StructField("id_motorista", T.StringType(), True),
        T.StructField("preco", T.DoubleType(), True),
    ])

    df = (
        spark.read
        .option("mergeSchema", "false")
        .schema(schema)
        .parquet(SILVER_PATH)
    )

    print("Schema lido com sucesso:")
    df.printSchema()

    print("Contando registros de entrada...")
    qtd = df.count()
    print(f"Qtd registros silver lidos: {qtd}")

    gold_df = (
        df
        .withColumn(
            "event_ts_ref",
            F.coalesce(
                F.col("event_ts"),
                F.col("ingest_ts"),
                F.current_timestamp()
            )
        )
        .withColumn("ano", F.year("event_ts_ref"))
        .withColumn("mes", F.month("event_ts_ref"))
        .withColumn("dia", F.dayofmonth("event_ts_ref"))
        .withColumn("hora", F.hour("event_ts_ref"))
        .groupBy("status", "fonte", "ano", "mes", "dia", "hora")
        .agg(
            F.count("*").alias("qtd_registros"),
            F.countDistinct("id_viagem").alias("qtd_viagens_distintas"),
            F.countDistinct("id_passageiro").alias("qtd_passageiros_distintos"),
            F.countDistinct("id_motorista").alias("qtd_motoristas_distintos"),
            F.avg("preco").alias("preco_medio"),
            F.sum("preco").alias("preco_total")
        )
        .withColumn("gold_process_ts", F.current_timestamp())
        .select(
            "status",
            "fonte",
            "qtd_registros",
            "qtd_viagens_distintas",
            "qtd_passageiros_distintos",
            "qtd_motoristas_distintos",
            "preco_medio",
            "preco_total",
            "gold_process_ts",
            "ano",
            "mes",
            "dia",
            "hora"
        )
    )

    print("Prévia do gold gerado:")
    gold_df.show(20, truncate=False)

    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("ano", "mes", "dia", "hora")
        .parquet(GOLD_PATH)
    )

    print(f"REBUILD OK: {GOLD_PATH}")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERRO NO REBUILD_VIAGENS_GOLD_V2")
        traceback.print_exc()
        sys.exit(1)
