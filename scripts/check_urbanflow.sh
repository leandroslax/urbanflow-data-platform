echo "===== SERVICES ====="
systemctl is-active urbanflow-*

echo
echo "===== KAFKA TEST ====="
timeout 5s ~/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server boot-mfuejbf1.c3.kafka-serverless.us-east-1.amazonaws.com:9098 \
--consumer.config ~/urbanflow-data-platform/config/client_iam.properties \
--topic urbanflow-viagens-bruto \
--max-messages 1

echo
echo "===== S3 FILES ====="
aws s3 ls s3://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/bronze/viagens/ | tail
