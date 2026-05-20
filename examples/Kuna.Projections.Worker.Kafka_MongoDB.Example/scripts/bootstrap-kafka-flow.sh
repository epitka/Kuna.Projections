#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/seed-kafka-live.sh"

echo "Kafka bootstrap complete."
echo "Kafka should now contain Kuna projection records produced by the seeder."
