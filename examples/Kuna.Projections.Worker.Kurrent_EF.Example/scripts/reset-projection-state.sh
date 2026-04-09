#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-kuna-showcase-postgres}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-orders_projection}"
POSTGRES_SCHEMA="${POSTGRES_SCHEMA:-dbo}"

echo "Clearing projection tables in container: ${POSTGRES_CONTAINER_NAME}"
echo "Database: ${POSTGRES_DB}, schema: ${POSTGRES_SCHEMA}"

docker exec -i "${POSTGRES_CONTAINER_NAME}" \
  psql \
  -v ON_ERROR_STOP=1 \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" <<SQL
TRUNCATE TABLE
  "${POSTGRES_SCHEMA}"."OrderRefunds",
  "${POSTGRES_SCHEMA}"."Orders",
  "${POSTGRES_SCHEMA}"."ProjectionFailures",
  "${POSTGRES_SCHEMA}"."CheckPoints"
RESTART IDENTITY
CASCADE;
SQL

echo "Projection state cleared. Kurrent events were left untouched."
