#!/usr/bin/env bash
# Start Docker Compose services needed through Phase N (1–5), cumulatively.
# Usage: bash infra/scripts/demo/start-stack-through-phase.sh [1|2|3|4|5]
# Requires: .env with KAFKA_CLUSTER_ID (see infra/scripts/init-kafka-storage.sh)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

PHASE="${1:-5}"
REPO="$(demo_repo_root)"
cd "${REPO}"

demo_load_dotenv "${REPO}"

if [[ -z "${KAFKA_CLUSTER_ID:-}" ]]; then
  echo "ERROR: KAFKA_CLUSTER_ID is unset. Run: bash infra/scripts/init-kafka-storage.sh"
  exit 1
fi

case "${PHASE}" in
  1)
    SERVICES=(kafka-1 kafka-2 kafka-3 schema-registry kafka-ui prometheus grafana)
    ;;
  2)
    SERVICES=(kafka-1 kafka-2 kafka-3 schema-registry kafka-ui prometheus grafana producer)
    ;;
  3)
    SERVICES=(
      kafka-1 kafka-2 kafka-3 schema-registry kafka-ui prometheus grafana
      producer postgres kafka-connect stream-processor
    )
    ;;
  4)
    SERVICES=(
      kafka-1 kafka-2 kafka-3 schema-registry kafka-ui prometheus grafana
      producer postgres kafka-connect stream-processor ksqldb-server
    )
    ;;
  5)
    SERVICES=(
      kafka-1 kafka-2 kafka-3 schema-registry kafka-ui prometheus grafana
      producer postgres kafka-connect stream-processor ksqldb-server ml-consumer
    )
    ;;
  *)
    echo "Usage: $0 [1|2|3|4|5]"
    exit 2
    ;;
esac

echo "Starting stack through Phase ${PHASE}: ${SERVICES[*]}"
docker compose up -d "${SERVICES[@]}"
echo "Done. Check: docker compose ps"
