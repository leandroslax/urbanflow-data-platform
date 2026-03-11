#!/bin/bash
set -euo pipefail

LOG_DIR="/home/ec2-user/urbanflow-data-platform/logs"
mkdir -p "${LOG_DIR}"

exec /home/ec2-user/spark/bin/spark-submit \
  --driver-memory 3g \
  --executor-memory 3g \
  --conf spark.sql.shuffle.partitions=24 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /home/ec2-user/urbanflow-data-platform/jobs/gold/stream_trafego_silver_to_gold_v1.py \
  >> "${LOG_DIR}/gold_trafego.log" 2>&1
