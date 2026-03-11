#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/clima_v1/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/clima_resumo_hora_v1/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-build-clima-gold-resumo-hora")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[GOLD-CLIMA] Lendo dados da silver...")
    df = spark.read.parquet(SILVER_PATH)

    cols = df.columns

    if "cidade" not in cols:
        df = df.withColumn("cidade", F.lit("desconhecida"))

    if "condicao_climatica" in cols:
        cond_col = "condicao_climatica"
    elif "condicao" in cols:
        cond_col = "condicao"
    else:
        df = df.withColumn("condicao_climatica", F.lit("nao_informada"))
        cond_col = "condicao_climatica"

    if "temperatura" not in cols:
        df = df.withColumn("temperatura", F.lit(None).cast("double"))

    if "umidade" not in cols:
        df = df.withColumn("umidade", F.lit(None).cast("double"))

    if "precipitacao" not in cols:
        df = df.withColumn("precipitacao", F.lit(None).cast("double"))

    gold_df = (
        df.groupBy("cidade", cond_col, "dt", "hora")
        .agg(
            F.count("*").alias("qtd_eventos_clima"),
            F.avg("temperatura").alias("temperatura_media"),
            F.max("temperatura").alias("temperatura_max"),
            F.min("temperatura").alias("temperatura_min"),
            F.avg("umidade").alias("umidade_media"),
            F.sum("precipitacao").alias("precipitacao_total")
        )
        .withColumn("gold_process_ts", F.current_timestamp())
        .withColumnRenamed(cond_col, "condicao_climatica")
        .withColumnRenamed("dt", "dt_ref")
        .withColumnRenamed("hora", "hora_ref")
    )

    print("[GOLD-CLIMA] Gravando no S3...")
    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("dt_ref", "hora_ref")
        .parquet(GOLD_PATH)
    )

    print("[GOLD-CLIMA] Reconstrução concluída com sucesso.")

if __name__ == "__main__":
    main()
