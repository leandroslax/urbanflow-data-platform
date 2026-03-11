#!/bin/bash
set -e

exec /home/ec2-user/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /home/ec2-user/urbanflow-data-platform/jobs/silver/stream_viagens_bronze_to_silver.py
