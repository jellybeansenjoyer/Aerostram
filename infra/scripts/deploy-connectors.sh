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
  # Spaces around "=" break Bash assignment lines (e.g. POSTGRES_PASSWORD = x sets nothing useful).
  if grep -qE '^[[:space:]]*POSTGRES_PASSWORD[[:space:]]+=' "${REPO_ROOT}/.env"; then
    echo "ERROR: In .env, do not put spaces around '=' for POSTGRES_PASSWORD."
    echo "  Wrong:  POSTGRES_PASSWORD = aerostream_secret"
    echo "  Right:  POSTGRES_PASSWORD=aerostream_secret"
    exit 1
  fi
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

# Match docker-compose: empty or unset → same default as POSTGRES_PASSWORD:-aerostream_secret
# Strip CRLF / stray whitespace — Windows .env line endings break JDBC in Kafka Connect while psql may still pass.
_raw_pw="${POSTGRES_PASSWORD:-aerostream_secret}"
POSTGRES_PASSWORD="$(
  python3 -c "import sys; s=(sys.argv[1] or '').strip().replace('\r','').replace('\n',''); print(s if s else 'aerostream_secret')" "${_raw_pw}"
)"
export POSTGRES_PASSWORD
unset _raw_pw

KC_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
# Avoid hangs if Connect is down or REST handler stalls (curl default = wait forever).
CURL_WAIT_SEC="${CURL_WAIT_SEC:-10}"
CURL_API_SEC="${CURL_API_SEC:-120}"

echo "================================================"
echo " AeroStream — CDC Connector Deployer"
echo " Connect URL: ${KC_URL}"
echo " Working dir: ${REPO_ROOT}"
echo "================================================"
echo " Postgres password for connectors: POSTGRES_PASSWORD from .env (see docker-compose postgres)."
echo " Using password length: ${#POSTGRES_PASSWORD} chars (value not printed)."
echo "================================================"

echo "Waiting for Kafka Connect REST API (${KC_URL})..."
CONNECT_ATTEMPTS=0
CONNECT_MAX_ATTEMPTS="${CONNECT_MAX_ATTEMPTS:-120}"
until curl -sf --max-time "${CURL_WAIT_SEC}" "${KC_URL}/connectors" > /dev/null; do
  CONNECT_ATTEMPTS=$((CONNECT_ATTEMPTS + 1))
  if [[ "${CONNECT_ATTEMPTS}" -ge "${CONNECT_MAX_ATTEMPTS}" ]]; then
    echo ""
    echo "ERROR: Kafka Connect did not respond OK within $((CONNECT_MAX_ATTEMPTS * 5))s."
    echo "  Start or fix Connect: docker compose ps kafka-connect && docker compose logs kafka-connect --tail 30"
    exit 1
  fi
  echo "  not ready, retrying in 5s... (${CONNECT_ATTEMPTS}/${CONNECT_MAX_ATTEMPTS})"
  sleep 5
done
echo "Kafka Connect is ready."
echo ""

echo "Verifying Postgres accepts TCP on 5432 (same path Debezium uses from kafka-connect)..."
if ! docker compose exec -T -e "PGPASSWORD=${POSTGRES_PASSWORD}" postgres \
  psql -h 127.0.0.1 -p 5432 -U aerostream -d aerostream -c 'SELECT 1' >/dev/null 2>&1; then
  echo "ERROR: TCP authentication failed for user aerostream with POSTGRES_PASSWORD from .env."
  echo "  Kafka Connect uses TCP (like this check). Fix one of:"
  echo "  1) Set POSTGRES_PASSWORD in .env to match the password Postgres was initialized with."
  echo "  2) Or reset local DB (DESTROYS DATA): docker compose stop postgres && docker volume rm <project>_postgres-data && docker compose up -d postgres"
  echo "     then wait for healthy Postgres and re-run this script."
  exit 1
fi
echo "Postgres TCP credential check OK (matches Debezium)."
echo ""

# Merge POSTGRES_PASSWORD into connector JSON (connector files cannot stay in sync with .env otherwise).
connector_json_payload() {
  local FILE="$1"
  python3 - "${FILE}" << 'PY'
import json, os, sys
path = sys.argv[1]
_raw = os.environ.get("POSTGRES_PASSWORD") or "aerostream_secret"
pw = _raw.strip().replace("\r", "").replace("\n", "")
if not pw:
    pw = "aerostream_secret"
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
_raw = os.environ.get("POSTGRES_PASSWORD") or "aerostream_secret"
pw = _raw.strip().replace("\r", "").replace("\n", "")
if not pw:
    pw = "aerostream_secret"
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
  HTTP_STATUS=$(connector_json_payload "${FILE}" | curl -sS --max-time "${CURL_API_SEC}" -o "${err_body}" -w "%{http_code}" \
    -X POST "${KC_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @-)

  case "${HTTP_STATUS}" in
    201)
      echo "CREATED"
      ;;
    409)
      echo -n "EXISTS, applying config update... "
      HTTP_PUT=$(connector_config_only "${FILE}" | curl -sS --max-time "${CURL_API_SEC}" -o "${err_body}" -w "%{http_code}" \
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
      if grep -q "password authentication failed" "${err_body}" 2>/dev/null; then
        echo ""
        echo "  Hint: POSTGRES_PASSWORD in .env must match the password Postgres was initialized with"
        echo "  (see postgres-data volume). Typo in .env (e.g. aerostrean vs aerostream_secret), Windows CRLF,"
        echo "  or changing .env after first init without ALTER USER causes this. Compare with:"
        echo "    docker compose exec postgres printenv POSTGRES_PASSWORD"
        echo "  (length only) echo -n \"\$POSTGRES_PASSWORD\" | wc -c"
      fi
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
CIRCUITS_STATE=$(curl -sS --max-time 30 "${KC_URL}/connectors/aerostream-circuits-connector/status" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
DRIVERS_STATE=$(curl -sS --max-time 30 "${KC_URL}/connectors/aerostream-drivers-connector/status" \
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
