#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/viagens_v2/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/viagens_resumo_hora_v1/"

def pick_first(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-build-viagens-gold-resumo-hora")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[GOLD-VIAGENS] Lendo dados da silver...")
    df = spark.read.parquet(SILVER_PATH)

    cols = set(df.columns)

    # Chaves temporais
    if all(c in cols for c in ["ano", "mes", "dia", "hora"]):
        df = (
            df.withColumn(
                "dt_ref",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("ano").cast("string"),
                        F.lpad(F.col("mes").cast("string"), 2, "0"),
                        F.lpad(F.col("dia").cast("string"), 2, "0")
                    )
                )
            )
            .withColumn("hora_ref", F.col("hora").cast("int"))
        )
    elif all(c in cols for c in ["dt_ref", "hora_ref"]):
        df = df.withColumn("dt_ref", F.to_date("dt_ref")).withColumn("hora_ref", F.col("hora_ref").cast("int"))
    else:
        raise Exception("Não encontrei colunas de tempo esperadas na silver de viagens.")

    viagem_col = pick_first(cols, ["viagem_id", "id_viagem", "trip_id"])
    passageiro_col = pick_first(cols, ["passageiro_id", "id_passageiro", "passenger_id"])
    motorista_col = pick_first(cols, ["motorista_id", "id_motorista", "driver_id"])
    preco_col = pick_first(cols, ["preco", "valor", "valor_total", "preco_total"])

    if "status" not in cols:
        df = df.withColumn("status", F.lit("desconhecido"))

    if "fonte" not in cols:
        df = df.withColumn("fonte", F.lit("desconhecida"))

    aggs = [F.count("*").alias("qtd_registros")]

    if viagem_col:
        aggs.append(F.countDistinct(F.col(viagem_col)).alias("qtd_viagens_distintas"))
    else:
        aggs.append(F.lit(None).cast("bigint").alias("qtd_viagens_distintas"))

    if passageiro_col:
        aggs.append(F.countDistinct(F.col(passageiro_col)).alias("qtd_passageiros_distintos"))
    else:
        aggs.append(F.lit(None).cast("bigint").alias("qtd_passageiros_distintos"))

    if motorista_col:
        aggs.append(F.countDistinct(F.col(motorista_col)).alias("qtd_motoristas_distintos"))
    else:
        aggs.append(F.lit(None).cast("bigint").alias("qtd_motoristas_distintos"))

    if preco_col:
        aggs.append(F.avg(F.col(preco_col).cast("double")).alias("preco_medio"))
        aggs.append(F.sum(F.col(preco_col).cast("double")).alias("preco_total"))
    else:
        aggs.append(F.lit(None).cast("double").alias("preco_medio"))
        aggs.append(F.lit(None).cast("double").alias("preco_total"))

    gold_df = (
        df.groupBy("dt_ref", "hora_ref", "status", "fonte")
        .agg(*aggs)
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
            "dt_ref",
            "hora_ref"
        )
    )

    print("[GOLD-VIAGENS] Gravando no S3...")
    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("dt_ref", "hora_ref")
        .parquet(GOLD_PATH)
    )

    print("[GOLD-VIAGENS] Reconstrução do gold concluída com sucesso.")

if __name__ == "__main__":
    main()
