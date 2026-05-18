#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/seed-kurrent-live.sh"

echo "KurrentDB -> Kafka bootstrap complete."
echo "Kafka should now contain records exported by the KurrentDB Kafka Sink connector."
