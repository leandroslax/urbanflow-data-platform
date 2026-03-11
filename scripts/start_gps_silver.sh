#!/bin/bash
set -euo pipefail

LOG_DIR="/home/ec2-user/urbanflow-data-platform/logs"
mkdir -p "${LOG_DIR}"

exec /home/ec2-user/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /home/ec2-user/urbanflow-data-platform/jobs/silver/stream_gps_bronze_to_silver.py \
  >> "${LOG_DIR}/gps_silver.log" 2>&1
