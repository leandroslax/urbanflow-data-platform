#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-boot-mfuejbf1.c3.kafka-serverless.us-east-1.amazonaws.com:9098}"
CLIENT_PROPS="/home/ec2-user/urbanflow-data-platform/config/client_iam.properties"
APP_DIR="/home/ec2-user/urbanflow-data-platform/apps/producers"

cd "$APP_DIR"

# Opcional: loga o ambiente básico (ajuda troubleshooting no journalctl)
echo "[INFO] BOOTSTRAP=$BOOTSTRAP"
echo "[INFO] CLIENT_PROPS=$CLIENT_PROPS"
echo "[INFO] APP_DIR=$APP_DIR"
ls -l ./urbanflow_producer.py || true
head -1 ./urbanflow_producer.py || true


# TomTom (tráfego) - opcional
TRAFFIC_REGIONS="/home/ec2-user/urbanflow-data-platform/config/traffic_regions.json"
TOMTOM_API_KEY="${TOMTOM_API_KEY:-}"

TRAFFIC_ARGS=()
if [[ -n "${TOMTOM_API_KEY}" ]]; then
  echo "[INFO] TomTom tráfego habilitado (TOMTOM_API_KEY definido)"
  TRAFFIC_ARGS+=(--trafego-enabled --tomtom-key "${TOMTOM_API_KEY}" --traffic-regions "${TRAFFIC_REGIONS}")
else
  echo "[INFO] TomTom tráfego desabilitado (defina TOMTOM_API_KEY para habilitar)"
fi

# Executa via python (evita noexec/permission denied do ./script)
exec /usr/bin/env python3 ./urbanflow_producer.py \
  --bootstrap "$BOOTSTRAP" \
  --client-props "$CLIENT_PROPS" \
  --modo misto \
  --dias 30 \
  "${TRAFFIC_ARGS[@]}"
