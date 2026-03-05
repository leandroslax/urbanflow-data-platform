from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

S3_BASE = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow"

def main():
    spark = SparkSession.builder.appName("urbanflow_silver_viagens").getOrCreate()

    bronze_path = f"{S3_BASE}/bronze/viagens"
    silver_path = f"{S3_BASE}/silver/viagens"

    df = spark.read.parquet(bronze_path)

    # Normalização leve (ajuste conforme schema real)
    if "ts" in df.columns:
        df = df.withColumn("event_ts", to_timestamp(col("ts")))

    # Deduplicação
    pk = "trip_id"
    if pk in df.columns:
        df = df.dropDuplicates([pk])
    else:
        df = df.dropDuplicates()

    (df.write
       .mode("append")
       .parquet(silver_path))

    spark.stop()

if __name__ == "__main__":
    main()
