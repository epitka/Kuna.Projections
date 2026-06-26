#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd -- "${EXAMPLE_DIR}/../.." && pwd)"

TARGET_EVENTS="${TARGET_EVENTS:-50000}"
MIN_COMPLETE_ORDERS="${MIN_COMPLETE_ORDERS:-3000}"
STREAM_PREFIX="${STREAM_PREFIX:-order-}"
REPORT_PATH="${REPORT_PATH:-/tmp/kuna-kafka-seed-report.json}"
KAFKA_TOPIC="${KAFKA_TOPIC:-orders-events}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-3}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
SEED="${SEED:-}"

seed_args=()
if [[ -n "${SEED}" ]]; then
  seed_args+=(--seed "${SEED}")
fi

echo "Ensuring Kafka topic exists."
docker compose -f "${EXAMPLE_DIR}/docker-compose.yml" exec -T redpanda \
  rpk topic create "${KAFKA_TOPIC}" --partitions "${KAFKA_PARTITIONS}" >/dev/null 2>&1 \
  || true

echo "Seeding Kafka at: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "Topic: ${KAFKA_TOPIC}"
echo "Target events: ${TARGET_EVENTS}, min complete orders: ${MIN_COMPLETE_ORDERS}"

dotnet run \
  --project "${REPO_ROOT}/examples/Kuna.Examples.EventsSeeder/Kuna.Examples.EventsSeeder.csproj" \
  -- \
  --kafka-bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
  --kafka-topic "${KAFKA_TOPIC}" \
  --target-events "${TARGET_EVENTS}" \
  --min-complete-orders "${MIN_COMPLETE_ORDERS}" \
  --stream-prefix "${STREAM_PREFIX}" \
  --report-path "${REPORT_PATH}" \
  "${seed_args[@]}"

echo "Live seed complete."
echo "Generation report: ${REPORT_PATH}"
echo "Kafka topic contains Kuna projection records for stream prefix: ${STREAM_PREFIX}"
