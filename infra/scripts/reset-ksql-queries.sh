#!/bin/bash
# Drops Phase 4 ksqlDB objects (terminates persistent queries first).
# Does NOT delete enriched-telemetry; optionally deletes stream-aggregates via DELETE TOPIC.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export KSQL_URL="http://localhost:${KSQL_PORT:-8088}"

python3 << 'PY'
import json, os, sys, urllib.request

ksql_url = os.environ["KSQL_URL"]

def post_ksql(statement: str):
    body = json.dumps({"ksql": statement}).encode()
    req = urllib.request.Request(
        f"{ksql_url}/ksql",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())

def ignore(statement: str):
    try:
        post_ksql(statement)
    except Exception as e:
        print("(ignored)", statement[:72], "->", e)

def collect_query_ids(obj):
    ids = []
    if isinstance(obj, dict):
        if "queries" in obj and isinstance(obj["queries"], list):
            for q in obj["queries"]:
                if isinstance(q, dict) and q.get("id"):
                    ids.append(q["id"])
        for v in obj.values():
            ids.extend(collect_query_ids(v))
    elif isinstance(obj, list):
        for x in obj:
            ids.extend(collect_query_ids(x))
    return ids

print("Terminating persistent queries...")
try:
    out = post_ksql("SHOW QUERIES;")
except Exception as e:
    print("SHOW QUERIES failed:", e)
    sys.exit(1)

ids = collect_query_ids(out)
seen = set()
for qid in ids:
    if qid in seen:
        continue
    seen.add(qid)
    print(" TERMINATE", qid)
    ignore(f"TERMINATE {qid};")

print("Dropping aggregate table (sink topic retained)...")
ignore("DROP TABLE IF EXISTS AGGREGATE_METRICS;")

print("Dropping source stream registration...")
ignore("DROP STREAM IF EXISTS ENRICHED_EVENTS;")

print("Reset finished. You can run: bash infra/scripts/deploy-ksql-queries.sh")
PY
