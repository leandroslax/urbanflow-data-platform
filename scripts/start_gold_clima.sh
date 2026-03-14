#!/bin/bash
set -euo pipefail

LOG_DIR="/home/ec2-user/urbanflow-data-platform/logs"
mkdir -p "${LOG_DIR}"

exec /home/ec2-user/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /home/ec2-user/urbanflow-data-platform/jobs/gold/stream_clima_silver_to_gold.py \
  >> "${LOG_DIR}/gold_clima.log" 2>&1
