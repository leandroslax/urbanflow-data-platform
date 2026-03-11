#!/bin/bash

LOG_FILE="/home/ec2-user/urbanflow-data-platform/logs/gold_gps_batch.log"

while true
do

echo "==================================================" >> $LOG_FILE
echo "[GOLD-GPS] $(date '+%Y-%m-%d %H:%M:%S') iniciando rebuild do gold" >> $LOG_FILE

/home/ec2-user/spark/bin/spark-submit \
--conf spark.driver.memory=3g \
--conf spark.sql.adaptive.enabled=false \
--conf spark.sql.shuffle.partitions=8 \
--executor-memory 3g \
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
/home/ec2-user/urbanflow-data-platform/jobs/gold/build_gps_gold_resumo_hora.py \
>> $LOG_FILE 2>&1

echo "[GOLD-GPS] $(date '+%Y-%m-%d %H:%M:%S') rebuild concluído. Aguardando 300 segundos..." >> $LOG_FILE

sleep 300

done
