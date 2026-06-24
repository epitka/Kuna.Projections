#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd -- "${EXAMPLE_DIR}/../.." && pwd)"

ESDB_BASE_URL="${ESDB_BASE_URL:-http://localhost:3000}"
ESDB_API_TOKEN="${ESDB_API_TOKEN:-secret}"
ESDB_BATCH_SIZE="${ESDB_BATCH_SIZE:-500}"
MAX_TARGET_EVENTS=25000
TARGET_EVENTS="${TARGET_EVENTS:-${MAX_TARGET_EVENTS}}"
MIN_COMPLETE_ORDERS="${MIN_COMPLETE_ORDERS:-3000}"
STREAM_PREFIX="${STREAM_PREFIX:-order-}"
REPORT_PATH="${REPORT_PATH:-/tmp/kuna-esdb-seed-report.json}"

if (( TARGET_EVENTS > MAX_TARGET_EVENTS )); then
  echo "TARGET_EVENTS cannot exceed ${MAX_TARGET_EVENTS} for EventSourcingDB; limiting to ${MAX_TARGET_EVENTS}."
  TARGET_EVENTS="${MAX_TARGET_EVENTS}"
fi

echo "Seeding EventSourcingDB at: ${ESDB_BASE_URL}"
echo "Target events: ${TARGET_EVENTS}, min complete orders: ${MIN_COMPLETE_ORDERS}, batch size: ${ESDB_BATCH_SIZE}"

dotnet run \
  --project "${REPO_ROOT}/examples/Kuna.Examples.EventsSeeder/Kuna.Examples.EventsSeeder.csproj" \
  -- \
  --esdb-base-url "${ESDB_BASE_URL}" \
  --esdb-api-token "${ESDB_API_TOKEN}" \
  --esdb-batch-size "${ESDB_BATCH_SIZE}" \
  --target-events "${TARGET_EVENTS}" \
  --min-complete-orders "${MIN_COMPLETE_ORDERS}" \
  --stream-prefix "${STREAM_PREFIX}" \
  --report-path "${REPORT_PATH}"

echo "Live seed complete."
echo "Generation report: ${REPORT_PATH}"
echo "EventSourcingDB contains Kuna projection events under subject root: /orders"
