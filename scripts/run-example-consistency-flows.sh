#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

EXAMPLES=(
  "Kuna.Projections.Worker.EventSourcingDB_Postgres.Example"
  "Kuna.Projections.Worker.Kafka_MongoDB.Example"
  "Kuna.Projections.Worker.Kurrent_MongoDB.Example"
  "Kuna.Projections.Worker.Kurrent_Postgres.Example"
)

current_example_dir=""

stop_current_infrastructure()
{
  if [[ -z "${current_example_dir}" ]]; then
    return
  fi

  docker compose \
    -f "${current_example_dir}/docker-compose.yml" \
    down -v --remove-orphans

  current_example_dir=""
}

cleanup()
{
  stop_current_infrastructure || true
}

trap cleanup EXIT INT TERM

for example in "${EXAMPLES[@]}"; do
  current_example_dir="${REPO_ROOT}/examples/${example}"

  echo
  echo "Running consistency flow: ${example}"
  echo

  "${current_example_dir}/scripts/run-live-consistency-flow.sh"
  stop_current_infrastructure
done

echo
echo "All example consistency flows completed with no inconsistencies."
