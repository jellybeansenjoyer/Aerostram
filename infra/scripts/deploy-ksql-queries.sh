#!/bin/bash
# Deploys Phase 4 ksqlDB persistent queries (ENRICHED_EVENTS stream + AGGREGATE_METRICS table).
# Prerequisites: docker compose up -d (ksqldb-server healthy), create-topics.sh, enriched-telemetry
# topic registered in Schema Registry (run producer + stream-processor at least briefly).
#
# Idempotency: if objects exist, run: bash infra/scripts/reset-ksql-queries.sh && re-run this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

KSQL_URL="http://localhost:${KSQL_PORT:-8088}"
KSQL_DIR="${REPO_ROOT}/infra/ksqldb"

echo "================================================"
echo " AeroStream — ksqlDB query deployer"
echo " ksqlDB REST: ${KSQL_URL}"
echo "================================================"

echo "Waiting for ksqlDB..."
until curl -sf "${KSQL_URL}/info" >/dev/null 2>&1; do
  echo "  not ready, retrying in 3s..."
  sleep 3
done
echo "ksqlDB is ready."
echo ""

export KSQL_DEPLOY_URL="${KSQL_URL}"
export KSQL_DEPLOY_DIR="${KSQL_DIR}"

python3 << 'PY'
import json, os, sys, urllib.error, urllib.request
from pathlib import Path

ksql_url = os.environ["KSQL_DEPLOY_URL"]
ksql_dir = Path(os.environ["KSQL_DEPLOY_DIR"])

def post_ksql(statement: str) -> dict:
    body = json.dumps({
        "ksql": statement,
        "streamsProperties": {
            "ksql.streams.auto.offset.reset": "earliest",
        },
    }).encode()
    req = urllib.request.Request(
        f"{ksql_url}/ksql",
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())

def check_errors(response) -> None:
    if isinstance(response, list):
        for item in response:
            if not isinstance(item, dict):
                continue
            if item.get("@type") == "statement_error":
                raise RuntimeError(item.get("message", str(item)))
            ent = item.get("commandStatus") or {}
            if isinstance(ent, dict) and ent.get("status") == "ERROR":
                raise RuntimeError(ent.get("message", str(item)))
    elif isinstance(response, dict):
        if response.get("@type") == "statement_error":
            raise RuntimeError(response.get("message", str(response)))

files = sorted(ksql_dir.glob("*.sql"))
if not files:
    print("No .sql files in", ksql_dir, file=sys.stderr)
    sys.exit(1)

for path in files:
    sql_text = path.read_text(encoding="utf-8")
    # strip line comments for display; send full file as one statement if single statement
    statements = []
    buf = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buf.append(line)
    cleaned = "\n".join(buf).strip()
    if not cleaned:
        continue
    # One statement per file expected
    print(f"Executing {path.name} ...")
    try:
        out = post_ksql(cleaned + ("\n" if not cleaned.endswith(";") else ""))
        check_errors(out)
        print(f"  OK")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode(errors="replace")
        print(f"  HTTP {e.code}: {err_body[:2000]}", file=sys.stderr)
        raise
PY

echo ""
echo "Done. Verify with:"
echo "  curl -s ${KSQL_URL}/ksql -H 'Content-Type: application/json' -d '{\"ksql\":\"SHOW QUERIES;\"}' | python3 -m json.tool"
