#!/usr/bin/env bash
set -euo pipefail

POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-orders_projection}"
POSTGRES_SCHEMA="${POSTGRES_SCHEMA:-dbo}"
POSTGRES_SERVICE_NAME="${POSTGRES_SERVICE_NAME:-postgres}"

echo "Clearing projection tables in compose service: ${POSTGRES_SERVICE_NAME}"
echo "Database: ${POSTGRES_DB}, schema: ${POSTGRES_SCHEMA}"

docker compose exec -T "${POSTGRES_SERVICE_NAME}" \
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
