#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-boot-mfuejbf1.c3.kafka-serverless.us-east-1.amazonaws.com:9098}"
CLIENT_PROPS="/home/ec2-user/urbanflow-data-platform/config/client_iam.properties"
APP_DIR="/home/ec2-user/urbanflow-data-platform/apps/producers"
LOG_DIR="/home/ec2-user/urbanflow-data-platform/logs"

mkdir -p "$LOG_DIR"

cd "$APP_DIR"

echo "[INFO] $(date) Iniciando UrbanFlow Producer"
echo "[INFO] BOOTSTRAP=$BOOTSTRAP"
echo "[INFO] CLIENT_PROPS=$CLIENT_PROPS"
echo "[INFO] APP_DIR=$APP_DIR"

ls -l ./urbanflow_producer.py || true
head -1 ./urbanflow_producer.py || true

TRAFFIC_REGIONS="/home/ec2-user/urbanflow-data-platform/config/traffic_regions.json"
export TOMTOM_API_KEY="NVaVgAtamlsmPokmWM9HyuGz1Nan68Ep"
TOMTOM_API_KEY="${TOMTOM_API_KEY:-}"

TRAFFIC_ARGS=()

if [[ -n "${TOMTOM_API_KEY}" ]]; then
  echo "[INFO] TomTom tráfego habilitado"
  TRAFFIC_ARGS+=(--trafego-enabled --tomtom-key "${TOMTOM_API_KEY}" --traffic-regions "${TRAFFIC_REGIONS}")
else
  echo "[INFO] TomTom tráfego desabilitado"
fi

exec /usr/bin/env python3 ./urbanflow_producer.py \
  --bootstrap "$BOOTSTRAP" \
  --client-props "$CLIENT_PROPS" \
  --modo realtime \
  --dias 30 \
  "${TRAFFIC_ARGS[@]}"
