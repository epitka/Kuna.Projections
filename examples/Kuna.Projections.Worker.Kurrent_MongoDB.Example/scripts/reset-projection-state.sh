#!/usr/bin/env bash
set -euo pipefail

MONGODB_CONTAINER_NAME="${MONGODB_CONTAINER_NAME:-kuna-showcase-mongodb}"
MONGODB_DATABASE="${MONGODB_DATABASE:-orders_projection}"
ORDERS_COLLECTION_NAME="${ORDERS_COLLECTION_NAME:-orders_order}"
CHECKPOINTS_COLLECTION_NAME="${CHECKPOINTS_COLLECTION_NAME:-projection_checkpoints}"

echo "Clearing projection collections in container: ${MONGODB_CONTAINER_NAME}"
echo "Database: ${MONGODB_DATABASE}"
echo "Collections: ${ORDERS_COLLECTION_NAME}, ${CHECKPOINTS_COLLECTION_NAME}"

docker exec -i "${MONGODB_CONTAINER_NAME}" \
  mongosh "${MONGODB_DATABASE}" --quiet <<EOF
db.getCollection("${ORDERS_COLLECTION_NAME}").deleteMany({});
db.getCollection("${CHECKPOINTS_COLLECTION_NAME}").deleteMany({});
EOF

echo "Projection state cleared. Kurrent events were left untouched."
