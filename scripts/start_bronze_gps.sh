#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${1:-boot-mfuejbf1.c3.kafka-serverless.us-east-1.amazonaws.com:9098}"
CLIENT_PROPS="${2:-/home/ec2-user/urbanflow-data-platform/config/client_iam.properties}"

SPARK_HOME="/home/ec2-user/spark"
APP_PY="/home/ec2-user/urbanflow-data-platform/jobs/bronze/stream_gps_to_s3_bronze.py"

MSK_IAM_JAR="/home/ec2-user/urbanflow-data-platform/jars/aws-msk-iam-auth-1.1.9-all.jar"
POOL2_JAR="/home/ec2-user/.ivy2/jars/org.apache.commons_commons-pool2-2.12.0.jar"

exec "${SPARK_HOME}/bin/spark-submit" \
  --conf "spark.driver.extraClassPath=${POOL2_JAR}:${MSK_IAM_JAR}" \
  --conf "spark.executor.extraClassPath=${POOL2_JAR}:${MSK_IAM_JAR}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
  --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
  --packages \
"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.5.1,\
org.apache.commons:commons-pool2:2.12.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --jars "${POOL2_JAR},${MSK_IAM_JAR}" \
  "${APP_PY}" \
  --bootstrap "${BOOTSTRAP}" \
  --client-props "${CLIENT_PROPS}"
