#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/silver/incidentes/"
GOLD_PATH = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/gold/incidentes_resumo_hora_v1/"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-build-incidentes-gold-resumo-hora")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[GOLD-INCIDENTES] Lendo dados da silver...")
    df = spark.read.parquet(SILVER_PATH)

    # Ajuste aqui se o nome da coluna de tipo/severidade for diferente no seu schema
    tipo_col = "tipo"
    severidade_col = "severidade"

    cols = df.columns

    if tipo_col not in cols:
        df = df.withColumn(tipo_col, F.lit("nao_informado"))

    if severidade_col not in cols:
        df = df.withColumn(severidade_col, F.lit("nao_informada"))

    gold_df = (
        df.groupBy("cidade", tipo_col, severidade_col, "dt_ref", "hora_ref")
        .agg(
            F.count("*").alias("qtd_incidentes"),
            F.countDistinct("incidente_id").alias("qtd_incidentes_distintos")
        )
        .withColumn("gold_process_ts", F.current_timestamp())
    )

    print("[GOLD-INCIDENTES] Gravando no S3...")
    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("dt_ref", "hora_ref")
        .parquet(GOLD_PATH)
    )

    print("[GOLD-INCIDENTES] Reconstrução concluída com sucesso.")

if __name__ == "__main__":
    main()
