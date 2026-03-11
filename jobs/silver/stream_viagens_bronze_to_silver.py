from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, current_timestamp

BUCKET = "s3a://urbanflow-datalake-dev-us-east-1-139961319000"
BRONZE_PATH = f"{BUCKET}/urbanflow/bronze/viagens"
SILVER_PATH = f"{BUCKET}/urbanflow/silver/viagens"
CHECKPOINT_PATH = f"{BUCKET}/checkpoints/viagens_silver_v1"

def main():
    spark = (
        SparkSession.builder
        .appName("urbanflow-viagens-bronze-to-silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bronze_schema = spark.read.parquet(BRONZE_PATH).schema

    df = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .load(BRONZE_PATH)
    )

    df_silver = (
        df.withColumn("ts_evento", to_timestamp(col("ts_evento")))
          .filter(col("ts_evento").isNotNull())
          .withColumn("silver_process_ts", current_timestamp())
          .withColumn("ano", year(col("ts_evento")))
          .withColumn("mes", month(col("ts_evento")))
          .withColumn("dia", dayofmonth(col("ts_evento")))
    )

    query = (
        df_silver.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ano", "mes", "dia")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
