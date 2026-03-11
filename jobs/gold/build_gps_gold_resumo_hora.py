#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/gps_v4/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/gps_resumo_hora_v1/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-build-gps-gold-resumo-hora")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[GOLD-GPS] Lendo dados da silver...")
    df = spark.read.parquet(SILVER_PATH)

    cols = df.columns

    if "cidade" not in cols:
        df = df.withColumn("cidade", F.lit("desconhecida"))

    if "veiculo_id" not in cols:
        df = df.withColumn("veiculo_id", F.lit(None).cast("string"))

    if "velocidade_kmh" not in cols:
        df = df.withColumn("velocidade_kmh", F.lit(None).cast("double"))

    df = df.filter(
        F.col("ts_evento").isNotNull() &
        F.col("cidade").isNotNull() &
        F.col("veiculo_id").isNotNull()
    )

    gold_df = (
        df.groupBy("cidade", "dt_ref", "hora_ref")
        .agg(
            F.count("*").alias("qtd_eventos_gps"),
            F.countDistinct("veiculo_id").alias("qtd_veiculos_distintos"),
            F.avg("velocidade_kmh").alias("velocidade_media"),
            F.min("velocidade_kmh").alias("velocidade_min"),
            F.max("velocidade_kmh").alias("velocidade_max")
        )
        .withColumn("gold_process_ts", F.current_timestamp())
    )

    print("[GOLD-GPS] Gravando no S3...")
    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("dt_ref", "hora_ref")
        .parquet(GOLD_PATH)
    )

    print("[GOLD-GPS] Reconstrução concluída com sucesso.")

if __name__ == "__main__":
    main()
