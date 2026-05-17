#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"

MONGODB_DATABASE="${MONGODB_DATABASE:-orders_projection}"
ORDERS_COLLECTION_NAME="${ORDERS_COLLECTION_NAME:-orders_order}"
CHECKPOINTS_COLLECTION_NAME="${CHECKPOINTS_COLLECTION_NAME:-projection_checkpoints}"

if [[ -n "${MONGODB_CONTAINER_NAME:-}" ]]; then
  RESOLVED_MONGODB_CONTAINER_NAME="${MONGODB_CONTAINER_NAME}"
else
  container_id="$(docker compose -f "${COMPOSE_FILE}" ps -q mongodb)"

  if [[ -z "${container_id}" ]]; then
    echo "MongoDB container for service 'mongodb' is not running. Start it with: docker compose up -d mongodb" >&2
    exit 1
  fi

  RESOLVED_MONGODB_CONTAINER_NAME="$(docker inspect --format '{{.Name}}' "${container_id}" | sed 's#^/##')"
fi

echo "Clearing projection collections in container: ${RESOLVED_MONGODB_CONTAINER_NAME}"
echo "Database: ${MONGODB_DATABASE}"
echo "Collections: ${ORDERS_COLLECTION_NAME}, ${CHECKPOINTS_COLLECTION_NAME}"

docker exec -i "${RESOLVED_MONGODB_CONTAINER_NAME}" \
  mongosh "${MONGODB_DATABASE}" --quiet <<EOF
db.getCollection("${ORDERS_COLLECTION_NAME}").deleteMany({});
db.getCollection("${CHECKPOINTS_COLLECTION_NAME}").deleteMany({});
EOF

echo "Projection state cleared. Kurrent events were left untouched."
