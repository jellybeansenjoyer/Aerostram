#!/usr/bin/env bash
# Remove both Debezium connectors from Kafka Connect (e.g. after a bad password was stored).
# Then run: bash infra/scripts/deploy-connectors.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
if [[ -f "${REPO_ROOT}/.env" ]]; then set -a; # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"; set +a; fi
KC_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
for name in aerostream-circuits-connector aerostream-drivers-connector; do
  echo "Deleting ${name}..."
  curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X DELETE "${KC_URL}/connectors/${name}" || true
done
echo "Done. Deploy again with: bash infra/scripts/deploy-connectors.sh"
