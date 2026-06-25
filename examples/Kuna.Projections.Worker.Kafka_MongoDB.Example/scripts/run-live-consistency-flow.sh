#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd -- "${EXAMPLE_DIR}/../.." && pwd)"

export EXAMPLE_DIR
export SEED_SCRIPT="${SCRIPT_DIR}/seed-kafka-live.sh"
export WORKER_PROJECT="${EXAMPLE_DIR}/Kuna.Projections.Worker.Kafka_MongoDB.Example.csproj"
export WORKER_URL="${WORKER_URL:-http://127.0.0.1:5284}"
export SOURCE_PORT="${SOURCE_PORT:-9092}"
export SINK_KIND="mongodb"

exec "${REPO_ROOT}/examples/scripts/run-live-consistency-flow.sh"
