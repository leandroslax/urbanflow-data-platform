from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, avg

S3_BASE = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow"

def main():
    spark = SparkSession.builder.appName("urbanflow_gold_congestionamento_por_hora").getOrCreate()

    trafego = spark.read.parquet(f"{S3_BASE}/silver/trafego")

    df = trafego
    if "event_ts" in df.columns:
        df = df.withColumn("hour", hour(col("event_ts")))
    elif "ts" in df.columns:
        df = df.withColumn("hour", hour(to_timestamp(col("ts"))))

    if all(c in df.columns for c in ["city","hour","congestion_index"]):
        out = df.groupBy("city","hour").agg(avg(col("congestion_index")).alias("avg_congestion_index"))
    else:
        out = df

    out.write.mode("overwrite").parquet(f"{S3_BASE}/gold/congestionamento_por_hora")
    spark.stop()

if __name__ == "__main__":
    main()
