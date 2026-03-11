#!/bin/bash
set -euo pipefail

BOOTSTRAP="${1:-}"
CLIENT_PROPS="${2:-}"

POOL2_JAR="/home/ec2-user/.ivy2/jars/org.apache.commons_commons-pool2-2.12.0.jar"
MSK_IAM_JAR="/home/ec2-user/urbanflow-data-platform/jars/aws-msk-iam-auth-1.1.9-all.jar"

~/spark/bin/spark-submit \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.5.1,\
org.apache.commons:commons-pool2:2.12.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf "spark.driver.extraClassPath=${POOL2_JAR}:${MSK_IAM_JAR}" \
  --conf "spark.executor.extraClassPath=${POOL2_JAR}:${MSK_IAM_JAR}" \
  --jars "${POOL2_JAR},${MSK_IAM_JAR}" \
  /home/ec2-user/urbanflow-data-platform/jobs/bronze/stream_viagens_to_s3_bronze.py \
  --bootstrap "${BOOTSTRAP}" \
  --client-props "${CLIENT_PROPS}"
