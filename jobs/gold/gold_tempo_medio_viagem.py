from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, unix_timestamp, to_timestamp

S3_BASE = "s3a://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow"

def main():
    spark = SparkSession.builder.appName("urbanflow_gold_tempo_medio_viagem").getOrCreate()

    v = spark.read.parquet(f"{S3_BASE}/silver/viagens")

    cols = v.columns
    if "start_ts" in cols and "end_ts" in cols:
        df = v.withColumn("dur_s", unix_timestamp(col("end_ts")) - unix_timestamp(col("start_ts")))
    elif "inicio_viagem" in cols and "fim_viagem" in cols:
        df = (v.withColumn("start_ts", to_timestamp(col("inicio_viagem")))
               .withColumn("end_ts", to_timestamp(col("fim_viagem")))
               .withColumn("dur_s", unix_timestamp(col("end_ts")) - unix_timestamp(col("start_ts"))))
    else:
        df = v

    if "city" in df.columns and "dur_s" in df.columns:
        out = df.groupBy("city").agg(avg(col("dur_s")).alias("avg_trip_duration_seconds"))
    else:
        out = df

    out.write.mode("overwrite").parquet(f"{S3_BASE}/gold/tempo_medio_viagem")
    spark.stop()

if __name__ == "__main__":
    main()
