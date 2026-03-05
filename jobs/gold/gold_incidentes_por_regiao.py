from pyspark.sql import SparkSession
from pyspark.sql.functions import count

S3_BASE = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow"

def main():
    spark = SparkSession.builder.appName("urbanflow_gold_incidentes_por_regiao").getOrCreate()

    inc = spark.read.parquet(f"{S3_BASE}/silver/incidentes")

    if all(c in inc.columns for c in ["city","region"]):
        out = inc.groupBy("city","region").agg(count("*").alias("total_incidentes"))
    else:
        out = inc

    out.write.mode("overwrite").parquet(f"{S3_BASE}/gold/incidentes_por_regiao")
    spark.stop()

if __name__ == "__main__":
    main()
