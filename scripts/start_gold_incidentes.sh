#!/bin/bash
set -euo pipefail

/home/ec2-user/spark/bin/spark-submit \
  --conf spark.driver.memory=2g \
  --conf spark.sql.shuffle.partitions=24 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /home/ec2-user/urbanflow-data-platform/jobs/gold/stream_incidentes_silver_to_gold.py
