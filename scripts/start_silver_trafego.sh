#!/bin/bash
set -euo pipefail

PROJECT_HOME="/home/ec2-user/urbanflow-data-platform"
LOG_DIR="${PROJECT_HOME}/logs"

mkdir -p "${LOG_DIR}"

exec ~/spark/bin/spark-submit \
  --driver-memory 3g \
  --executor-memory 3g \
  --conf spark.sql.shuffle.partitions=8 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  "${PROJECT_HOME}/jobs/silver/stream_trafego_bronze_to_silver.py" \
  >> "${LOG_DIR}/silver_trafego.log" 2>&1
