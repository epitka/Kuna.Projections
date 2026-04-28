#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd -- "${EXAMPLE_DIR}/../.." && pwd)"

CONNECTION_STRING="${KURRENT_CONNECTION_STRING:-esdb://admin:changeit@localhost:2113?tls=false}"
TARGET_EVENTS="${TARGET_EVENTS:-50000}"
MIN_COMPLETE_ORDERS="${MIN_COMPLETE_ORDERS:-3000}"
STREAM_PREFIX="${STREAM_PREFIX:-order-}"
REPORT_PATH="${REPORT_PATH:-/tmp/kuna-es-ef-seed-report.json}"

echo "Seeding KurrentDB at: ${CONNECTION_STRING}"
echo "Target events: ${TARGET_EVENTS}, min complete orders: ${MIN_COMPLETE_ORDERS}"

dotnet run \
  --project "${REPO_ROOT}/examples/Kuna.Projections.Worker.Kurrent_EF.Example/Kuna.Projections.Worker.Kurrent_EF.Example.csproj" \
  -- \
  --seed \
  --connection-string "${CONNECTION_STRING}" \
  --target-events "${TARGET_EVENTS}" \
  --min-complete-orders "${MIN_COMPLETE_ORDERS}" \
  --stream-prefix "${STREAM_PREFIX}" \
  --report-path "${REPORT_PATH}"

echo "Live seed complete."
echo "Generation report: ${REPORT_PATH}"
