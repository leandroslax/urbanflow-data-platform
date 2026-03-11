#!/bin/bash
set -euo pipefail

LOG_FILE=/home/ec2-user/urbanflow-data-platform/logs/silver_gps_batch.log
mkdir -p /home/ec2-user/urbanflow-data-platform/logs

while true
do
  echo "==================================================" >> "$LOG_FILE"
  echo "[SILVER-GPS-V4] $(date '+%F %T') iniciando rebuild do silver" >> "$LOG_FILE"

  /home/ec2-user/spark/bin/spark-submit \
    --driver-memory 3g \
    --executor-memory 3g \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.adaptive.enabled=false \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /home/ec2-user/urbanflow-data-platform/jobs/silver/build_gps_bronze_to_silver_v4.py >> "$LOG_FILE" 2>&1

  echo "[SILVER-GPS-V4] $(date '+%F %T') rebuild concluído. Aguardando 300 segundos..." >> "$LOG_FILE"
  sleep 300
done
