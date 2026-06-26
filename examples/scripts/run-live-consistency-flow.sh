#!/usr/bin/env bash
set -euo pipefail

: "${EXAMPLE_DIR:?EXAMPLE_DIR is required}"
: "${SEED_SCRIPT:?SEED_SCRIPT is required}"
: "${WORKER_PROJECT:?WORKER_PROJECT is required}"
: "${WORKER_URL:?WORKER_URL is required}"
: "${SOURCE_PORT:?SOURCE_PORT is required}"
: "${SINK_KIND:?SINK_KIND is required}"

INITIAL_EVENTS="${INITIAL_EVENTS:-10000}"
SECOND_EVENTS="${SECOND_EVENTS:-5000}"
INITIAL_MIN_COMPLETE_ORDERS="${INITIAL_MIN_COMPLETE_ORDERS:-500}"
SECOND_MIN_COMPLETE_ORDERS="${SECOND_MIN_COMPLETE_ORDERS:-250}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-120}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-300}"
FLUSH_QUIET_SECONDS="${FLUSH_QUIET_SECONDS:-5}"
CONSISTENCY_TIMEOUT_SECONDS="${CONSISTENCY_TIMEOUT_SECONDS:-600}"
CONSISTENCY_SEED="${CONSISTENCY_SEED:-8675309}"
INITIAL_SEED="${INITIAL_SEED:-${CONSISTENCY_SEED}}"
SECOND_SEED="${SECOND_SEED:-$((CONSISTENCY_SEED + 1))}"
SOURCE_HOST="${SOURCE_HOST:-127.0.0.1}"
WORKER_LOG="${WORKER_LOG:-/tmp/$(basename "${EXAMPLE_DIR}")-consistency-flow.log}"
INITIAL_REPORT_PATH="${INITIAL_REPORT_PATH:-/tmp/$(basename "${EXAMPLE_DIR}")-initial-seed-report.json}"
SECOND_REPORT_PATH="${SECOND_REPORT_PATH:-/tmp/$(basename "${EXAMPLE_DIR}")-second-seed-report.json}"
CONSISTENCY_RESULT_PATH="${CONSISTENCY_RESULT_PATH:-/tmp/$(basename "${EXAMPLE_DIR}")-consistency-result.json}"

worker_pid=""

cleanup()
{
  if [[ -n "${worker_pid}" ]] && kill -0 "${worker_pid}" 2>/dev/null; then
    kill "${worker_pid}" 2>/dev/null || true
    wait "${worker_pid}" 2>/dev/null || true
  fi
}

fail()
{
  echo "$1" >&2

  if [[ -f "${WORKER_LOG}" ]]; then
    echo "Worker log: ${WORKER_LOG}" >&2
    tail -n 80 "${WORKER_LOG}" >&2 || true
  fi

  exit 1
}

require_command()
{
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "Required command not found: $1"
  fi
}

wait_for_source()
{
  local deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))

  echo "Waiting for source at ${SOURCE_HOST}:${SOURCE_PORT}."

  until (echo >/dev/tcp/"${SOURCE_HOST}"/"${SOURCE_PORT}") >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      fail "Source did not accept TCP connections within ${STARTUP_TIMEOUT_SECONDS}s."
    fi

    sleep 1
  done
}

wait_for_worker()
{
  local deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))

  echo "Waiting for worker at ${WORKER_URL}."

  until curl --fail --silent --show-error "${WORKER_URL}/" >/dev/null 2>&1; do
    if ! kill -0 "${worker_pid}" 2>/dev/null; then
      wait "${worker_pid}" || true
      fail "Worker exited before becoming ready."
    fi

    if ((SECONDS >= deadline)); then
      fail "Worker did not become ready within ${STARTUP_TIMEOUT_SECONDS}s."
    fi

    sleep 1
  done
}

get_projected_order_count()
{
  local output

  case "${SINK_KIND}" in
    postgres)
      output="$(
        docker compose -f "${EXAMPLE_DIR}/docker-compose.yml" exec -T "${POSTGRES_SERVICE_NAME:-postgres}" \
          psql \
          -v ON_ERROR_STOP=1 \
          -U "${POSTGRES_USER:-postgres}" \
          -d "${POSTGRES_DB:-orders_projection}" \
          -Atc "SELECT COUNT(*) FROM \"${POSTGRES_SCHEMA:-dbo}\".\"Orders\";"
      )"
      ;;
    mongodb)
      output="$(
        docker compose -f "${EXAMPLE_DIR}/docker-compose.yml" exec -T "${MONGODB_SERVICE_NAME:-mongodb}" \
          mongosh "${MONGODB_DATABASE:-orders_projection}" \
          --quiet \
          --eval "db.getCollection(\"${ORDERS_COLLECTION_NAME:-orders_order}\").countDocuments({})"
      )"
      ;;
    *)
      fail "Unsupported SINK_KIND: ${SINK_KIND}"
      ;;
  esac

  echo "${output}" | tr -d '[:space:]'
}

wait_until_initially_drained()
{
  local expected_count="$1"
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
  local actual_count
  local drain_count

  echo "Waiting for 'Projection pipeline fully drained' message."

  while true; do
    if ! kill -0 "${worker_pid}" 2>/dev/null; then
      wait "${worker_pid}" || true
      fail "Worker exited while waiting for the projection to drain."
    fi

    drain_count="$(grep -c "Projection pipeline fully drained" "${WORKER_LOG}" 2>/dev/null || true)"

    if ((drain_count >= 1)); then
      actual_count="$(get_projected_order_count 2>/dev/null || true)"

      if [[ "${actual_count}" != "${expected_count}" ]]; then
        fail "Projection reported fully drained with ${actual_count} persisted orders; expected exactly ${expected_count}."
      fi

      echo "Projection fully drained: ${actual_count} persisted orders."
      return
    fi

    if ((SECONDS >= deadline)); then
      fail "Worker did not log 'Projection pipeline fully drained' within ${DRAIN_TIMEOUT_SECONDS}s."
    fi

    sleep 1
  done
}

wait_for_projected_order_count()
{
  local expected_count="$1"
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
  local actual_count

  echo "Waiting for ${expected_count} persisted orders after the live seed."

  while true; do
    if ! kill -0 "${worker_pid}" 2>/dev/null; then
      wait "${worker_pid}" || true
      fail "Worker exited while processing the live seed."
    fi

    actual_count="$(get_projected_order_count 2>/dev/null || true)"

    if [[ "${actual_count}" == "${expected_count}" ]]; then
      return
    fi

    if [[ "${actual_count}" =~ ^[0-9]+$ ]] && ((actual_count > expected_count)); then
      fail "Projection contains ${actual_count} orders; expected exactly ${expected_count}."
    fi

    if ((SECONDS >= deadline)); then
      fail "Projection did not reach ${expected_count} persisted orders within ${DRAIN_TIMEOUT_SECONDS}s; last count was '${actual_count}'."
    fi

    sleep 1
  done
}

wait_for_projection_flush_quiet()
{
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
  local flush_count
  local previous_flush_count=-1
  local quiet_since=$SECONDS

  echo "Waiting for persisted projection flushes to remain quiet for ${FLUSH_QUIET_SECONDS}s."

  while true; do
    if ! kill -0 "${worker_pid}" 2>/dev/null; then
      wait "${worker_pid}" || true
      fail "Worker exited while waiting for persisted projection flushes to drain."
    fi

    flush_count="$(grep -c "Projection pipeline flush persisted" "${WORKER_LOG}" 2>/dev/null || true)"

    if ((flush_count != previous_flush_count)); then
      previous_flush_count="${flush_count}"
      quiet_since=$SECONDS
    elif ((SECONDS - quiet_since >= FLUSH_QUIET_SECONDS)); then
      echo "Projection fully drained: persisted flush count remained at ${flush_count}."
      return
    fi

    if ((SECONDS >= deadline)); then
      fail "Projection flush activity did not become quiet within ${DRAIN_TIMEOUT_SECONDS}s."
    fi

    sleep 1
  done
}

run_replay_consistency()
{
  echo "Running replay consistency check with a ${CONSISTENCY_TIMEOUT_SECONDS}s timeout."

  if ! curl \
    --fail \
    --show-error \
    --max-time "${CONSISTENCY_TIMEOUT_SECONDS}" \
    --request POST \
    "${WORKER_URL}/diagnostics/orders/replay-consistency" \
    --header "Content-Type: application/json" \
    --data '{"stopOnFirstMismatch":true,"logEvery":500}' \
    --output "${CONSISTENCY_RESULT_PATH}"; then
    fail "Replay consistency request failed or timed out."
  fi

  if ! jq --exit-status '.isConsistent == true and .mismatch == null' "${CONSISTENCY_RESULT_PATH}" >/dev/null; then
    jq . "${CONSISTENCY_RESULT_PATH}" >&2 || true
    fail "Replay consistency check reported inconsistencies."
  fi
}

seed_events()
{
  local target_events="$1"
  local minimum_complete_orders="$2"
  local report_path="$3"
  local seed="$4"

  SEED="${seed}" \
  TARGET_EVENTS="${target_events}" \
  MIN_COMPLETE_ORDERS="${minimum_complete_orders}" \
  REPORT_PATH="${report_path}" \
    "${SEED_SCRIPT}"
}

trap cleanup EXIT INT TERM

for command in curl docker dotnet jq; do
  require_command "${command}"
done

echo "Resetting compose infrastructure for $(basename "${EXAMPLE_DIR}")."
docker compose -f "${EXAMPLE_DIR}/docker-compose.yml" down -v --remove-orphans
docker compose -f "${EXAMPLE_DIR}/docker-compose.yml" up -d --wait
wait_for_source

echo "Seeding initial ${INITIAL_EVENTS} events with seed ${INITIAL_SEED}."
seed_events "${INITIAL_EVENTS}" "${INITIAL_MIN_COMPLETE_ORDERS}" "${INITIAL_REPORT_PATH}" "${INITIAL_SEED}"
initial_order_count="$(jq --exit-status --raw-output '.totalOrders' "${INITIAL_REPORT_PATH}")"

echo "Starting projection worker. Log: ${WORKER_LOG}"
ASPNETCORE_URLS="${WORKER_URL}" \
  dotnet run \
    -c Release \
    --no-launch-profile \
    --project "${WORKER_PROJECT}" \
    >"${WORKER_LOG}" 2>&1 &
worker_pid=$!

wait_for_worker
wait_until_initially_drained "${initial_order_count}"

echo "Seeding another ${SECOND_EVENTS} events with seed ${SECOND_SEED} while the worker is running."
seed_events "${SECOND_EVENTS}" "${SECOND_MIN_COMPLETE_ORDERS}" "${SECOND_REPORT_PATH}" "${SECOND_SEED}"
second_order_count="$(jq --exit-status --raw-output '.totalOrders' "${SECOND_REPORT_PATH}")"
expected_order_count=$((initial_order_count + second_order_count))

wait_for_projected_order_count "${expected_order_count}"
wait_for_projection_flush_quiet
run_replay_consistency

jq . "${CONSISTENCY_RESULT_PATH}"
echo "Consistency flow completed with no inconsistencies."
echo "Worker log: ${WORKER_LOG}"
echo "Consistency result: ${CONSISTENCY_RESULT_PATH}"
