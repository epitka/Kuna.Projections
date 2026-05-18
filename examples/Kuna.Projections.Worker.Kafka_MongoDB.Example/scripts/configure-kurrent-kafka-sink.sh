#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"

KURRENT_BASE_URL="${KURRENT_BASE_URL:-http://localhost:2113}"
KURRENT_USERNAME="${KURRENT_USERNAME:-admin}"
KURRENT_PASSWORD="${KURRENT_PASSWORD:-changeit}"
CONNECTOR_ID="${CONNECTOR_ID:-orders-kafka-sink}"
CONNECTOR_NAME="${CONNECTOR_NAME:-Orders Kafka Sink}"
KAFKA_TOPIC="${KAFKA_TOPIC:-orders-events}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-3}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-redpanda:9092}"
STREAM_PREFIX="${STREAM_PREFIX:-order-}"
INITIAL_POSITION="${INITIAL_POSITION:-earliest}"
WAIT_FOR_BROKER_ACK="${WAIT_FOR_BROKER_ACK:-true}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

wait_for_kurrent() {
  local attempt

  for attempt in $(seq 1 60); do
    if curl --silent --show-error --fail \
      --user "${KURRENT_USERNAME}:${KURRENT_PASSWORD}" \
      "${KURRENT_BASE_URL}/connectors" >/dev/null 2>&1; then
      return 0
    fi

    sleep 2
  done

  echo "Timed out waiting for KurrentDB connectors API at ${KURRENT_BASE_URL}" >&2
  exit 1
}

ensure_topic() {
  docker compose -f "${COMPOSE_FILE}" exec -T redpanda \
    rpk topic create "${KAFKA_TOPIC}" --partitions "${KAFKA_PARTITIONS}" >/dev/null 2>&1 \
    || true
}

connector_exists() {
  curl --silent --show-error --fail \
    --user "${KURRENT_USERNAME}:${KURRENT_PASSWORD}" \
    "${KURRENT_BASE_URL}/connectors/${CONNECTOR_ID}/settings" >/dev/null 2>&1
}

write_create_payload() {
  cat <<EOF
{
  "name": "${CONNECTOR_NAME}",
  "settings": {
    "instanceTypeName": "kafka-sink",
    "bootstrapServers": "${KAFKA_BOOTSTRAP_SERVERS}",
    "topic": "${KAFKA_TOPIC}",
    "subscription:filter:scope": "stream",
    "subscription:filter:filterType": "prefix",
    "subscription:filter:expression": "${STREAM_PREFIX}",
    "subscription:initialPosition": "${INITIAL_POSITION}",
    "partitionKeyExtraction:enabled": "true",
    "partitionKeyExtraction:source": "streamSuffix",
    "waitForBrokerAck": "${WAIT_FOR_BROKER_ACK}"
  }
}
EOF
}

write_settings_payload() {
  cat <<EOF
{
  "instanceTypeName": "kafka-sink",
  "bootstrapServers": "${KAFKA_BOOTSTRAP_SERVERS}",
  "topic": "${KAFKA_TOPIC}",
  "subscription:filter:scope": "stream",
  "subscription:filter:filterType": "prefix",
  "subscription:filter:expression": "${STREAM_PREFIX}",
  "subscription:initialPosition": "${INITIAL_POSITION}",
  "partitionKeyExtraction:enabled": "true",
  "partitionKeyExtraction:source": "streamSuffix",
  "waitForBrokerAck": "${WAIT_FOR_BROKER_ACK}"
}
EOF
}

configure_connector() {
  if connector_exists; then
    curl --silent --show-error \
      --user "${KURRENT_USERNAME}:${KURRENT_PASSWORD}" \
      -X POST "${KURRENT_BASE_URL}/connectors/${CONNECTOR_ID}/stop" >/dev/null 2>&1 \
      || true

    write_settings_payload | curl --silent --show-error --fail \
      --user "${KURRENT_USERNAME}:${KURRENT_PASSWORD}" \
      -H "Content-Type: application/json" \
      -X PUT \
      --data-binary @- \
      "${KURRENT_BASE_URL}/connectors/${CONNECTOR_ID}/settings" >/dev/null
  else
    write_create_payload | curl --silent --show-error --fail \
      --user "${KURRENT_USERNAME}:${KURRENT_PASSWORD}" \
      -H "Content-Type: application/json" \
      -X POST \
      --data-binary @- \
      "${KURRENT_BASE_URL}/connectors/${CONNECTOR_ID}" >/dev/null
  fi
}

start_connector() {
  curl --silent --show-error --fail \
    --user "${KURRENT_USERNAME}:${KURRENT_PASSWORD}" \
    -X POST "${KURRENT_BASE_URL}/connectors/${CONNECTOR_ID}/start" >/dev/null
}

require_command curl
require_command docker

wait_for_kurrent
ensure_topic
configure_connector
start_connector

echo "Configured Kafka sink connector '${CONNECTOR_ID}'."
echo "KurrentDB filter prefix: ${STREAM_PREFIX}"
echo "Kafka topic: ${KAFKA_TOPIC}"
echo "Kafka bootstrap servers seen by KurrentDB: ${KAFKA_BOOTSTRAP_SERVERS}"
