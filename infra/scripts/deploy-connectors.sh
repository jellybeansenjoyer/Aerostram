#!/bin/bash
# Idempotently deploys both Debezium CDC connectors to the Kafka Connect REST API.
# HTTP 201 = created, 409 = already exists (both are success).
# Run AFTER: docker compose up -d kafka-connect && kafka-connect is healthy.
#
# IMPORTANT: Do not use curl -f with POST here. On HTTP 400/409, -f makes curl exit
# non-zero; combining that with "|| echo 000" appends to -w "%{http_code}" output and
# produces bogus statuses like "400000".

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# Load repo .env so POSTGRES_PASSWORD matches the postgres container (same as docker compose).
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

KC_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"

echo "================================================"
echo " AeroStream — CDC Connector Deployer"
echo " Connect URL: ${KC_URL}"
echo " Working dir: ${REPO_ROOT}"
echo "================================================"
echo " Postgres password for connectors: from POSTGRES_PASSWORD env"
echo " (defaults to aerostream_secret; must match docker-compose postgres)."
echo "================================================"

echo "Waiting for Kafka Connect REST API..."
until curl -sf "${KC_URL}/connectors" > /dev/null; do
  echo "  not ready, retrying in 5s..."
  sleep 5
done
echo "Kafka Connect is ready."
echo ""

# Merge POSTGRES_PASSWORD into connector JSON (connector files cannot stay in sync with .env otherwise).
connector_json_payload() {
  local FILE="$1"
  python3 - "${FILE}" << 'PY'
import json, os, sys
path = sys.argv[1]
pw = os.environ.get("POSTGRES_PASSWORD", "aerostream_secret")
with open(path, encoding="utf-8") as f:
    doc = json.load(f)
doc["config"]["database.password"] = pw
json.dump(doc, sys.stdout)
PY
}

# Connect PUT /connectors/{name}/config expects the flat config object only.
connector_config_only() {
  local FILE="$1"
  python3 - "${FILE}" << 'PY'
import json, os, sys
path = sys.argv[1]
pw = os.environ.get("POSTGRES_PASSWORD", "aerostream_secret")
with open(path, encoding="utf-8") as f:
    doc = json.load(f)
doc["config"]["database.password"] = pw
json.dump(doc["config"], sys.stdout)
PY
}

deploy_connector() {
  local FILE="$1"
  local NAME
  NAME=$(python3 -c "import json,sys; print(json.load(open('${FILE}'))['name'])")
  echo -n "  Deploying ${NAME}... "

  local err_body
  err_body="$(mktemp)"
  # No -f: we need the real HTTP code even on 4xx, and must not chain || echo 000
  local HTTP_STATUS
  HTTP_STATUS=$(connector_json_payload "${FILE}" | curl -sS -o "${err_body}" -w "%{http_code}" \
    -X POST "${KC_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @-)

  case "${HTTP_STATUS}" in
    201)
      echo "CREATED"
      ;;
    409)
      echo -n "EXISTS, applying config update... "
      HTTP_PUT=$(connector_config_only "${FILE}" | curl -sS -o "${err_body}" -w "%{http_code}" \
        -X PUT "${KC_URL}/connectors/${NAME}/config" \
        -H "Content-Type: application/json" \
        -d @-)
      case "${HTTP_PUT}" in
        200)
          echo "OK"
          ;;
        *)
          echo "FAILED HTTP ${HTTP_PUT}"
          echo "  Response from Kafka Connect:"
          sed 's/^/  /' "${err_body}" | head -c 4000
          echo ""
          rm -f "${err_body}"
          exit 1
          ;;
      esac
      ;;
    000|"")
      echo "ERROR: could not reach Connect at ${KC_URL} (connection failed)."
      cat "${err_body}" 2>/dev/null || true
      rm -f "${err_body}"
      exit 1
      ;;
    *)
      echo "ERROR HTTP ${HTTP_STATUS}"
      echo "  Response from Kafka Connect:"
      sed 's/^/  /' "${err_body}" | head -c 4000
      echo ""
      rm -f "${err_body}"
      exit 1
      ;;
  esac
  rm -f "${err_body}"
}

deploy_connector "infra/kafka-connect/connectors/aerostream-circuits-connector.json"
deploy_connector "infra/kafka-connect/connectors/aerostream-drivers-connector.json"

echo ""
echo "Waiting 15s for initial CDC snapshot to complete..."
sleep 15

echo ""
echo "Connector statuses:"
CIRCUITS_STATE=$(curl -s "${KC_URL}/connectors/aerostream-circuits-connector/status" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
DRIVERS_STATE=$(curl -s "${KC_URL}/connectors/aerostream-drivers-connector/status" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")

echo "  aerostream-circuits-connector: ${CIRCUITS_STATE}"
echo "  aerostream-drivers-connector:  ${DRIVERS_STATE}"

if [ "${CIRCUITS_STATE}" = "RUNNING" ] && [ "${DRIVERS_STATE}" = "RUNNING" ]; then
  echo ""
  echo "Both connectors RUNNING. Initial snapshot complete."
  echo "circuit-metadata and driver-profiles topics are being populated."
else
  echo ""
  echo "WARNING: One or more connectors not in RUNNING state. Check logs:"
  echo "  docker compose logs kafka-connect | tail -50"
  exit 1
fi
