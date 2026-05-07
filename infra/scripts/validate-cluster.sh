#!/bin/bash
# AeroStream Phase 1 Cluster Validation Suite
# Prints PASS/FAIL for each check. Exits 0 only if all pass.

# Load repo .env from any cwd (port overrides: STREAM_PROCESSOR_PORT, KAFKA_CONNECT_PORT, …)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

PASS=0; FAIL=0

check() {
  local DESC="$1"
  local CMD="$2"
  printf "  %-55s" "$DESC"
  if eval "$CMD" &>/dev/null; then
    echo "[ PASS ]"
    ((PASS++))
  else
    echo "[ FAIL ]"
    ((FAIL++))
  fi
}

# Sum log-end offsets for a topic (handles colon and whitespace kafka-get-offsets output).
topic_has_records() {
  local topic="$1"
  local out
  out=$(docker exec -e KAFKA_OPTS='' kafka-1 kafka-get-offsets \
    --bootstrap-server kafka-1:9092 \
    --topic "$topic" 2>/dev/null) || return 1
  echo "$out" | python3 -c "
import re, sys
text = sys.stdin.read()
total = 0
for line in text.splitlines():
    line = line.strip()
    if not line or line.upper().startswith('TOPIC') or line.startswith('#'):
        continue
    m = re.match(r'^([^:]+):(\d+):(\d+)\s*$', line)
    if m:
        total += int(m.group(3))
        continue
    parts = line.split()
    if len(parts) >= 3 and parts[-1].isdigit() and parts[-2].isdigit():
        total += int(parts[-1])
sys.exit(0 if total > 0 else 1)
"
}

# Do not use curl -f: Spring may return non-2xx while aggregate status is still JSON-parseable;
# also avoids masking failures when /actuator/health returns 503 with a body.
stream_processor_health_up() {
  local port="${STREAM_PROCESSOR_PORT:-8091}"
  local body=""
  body=$(curl -sS --max-time 10 "http://127.0.0.1:${port}/actuator/health" 2>/dev/null) || true
  [ -z "$body" ] && body=$(curl -sS --max-time 10 "http://localhost:${port}/actuator/health" 2>/dev/null) || true
  [ -n "$body" ] || return 1
  echo "$body" | python3 -c "import json,sys; r=json.load(sys.stdin); sys.exit(0 if r.get('status')=='UP' else 1)" 2>/dev/null
}

# Connector-level RUNNING is not enough: tasks can be FAILED while the REST body still contains "RUNNING".
connect_connector_and_tasks_running() {
  local name="$1"
  local port="${KAFKA_CONNECT_PORT:-8083}"
  curl -sf "http://localhost:${port}/connectors/${name}/status" | python3 -c "
import json, sys
d = json.load(sys.stdin)
if d.get('connector', {}).get('state') != 'RUNNING':
    sys.exit(1)
tasks = d.get('tasks') or []
if not tasks:
    sys.exit(1)
for t in tasks:
    if t.get('state') != 'RUNNING':
        sys.exit(1)
sys.exit(0)
" 2>/dev/null
}

echo ""
echo "======================================================"
echo "  AeroStream — Cluster Validation (Phases 1–4)"
echo "======================================================"
echo ""

# ── Kafka Brokers ─────────────────────────────────────────────────────────────
echo "[ Kafka Cluster ]"
check "kafka-1 broker reachable" \
  "docker exec -e KAFKA_OPTS='' kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:9092"

check "kafka-2 broker reachable" \
  "docker exec -e KAFKA_OPTS='' kafka-2 kafka-broker-api-versions --bootstrap-server kafka-2:9092"

check "kafka-3 broker reachable" \
  "docker exec -e KAFKA_OPTS='' kafka-3 kafka-broker-api-versions --bootstrap-server kafka-3:9092"

check "No ZooKeeper containers running" \
  "! docker ps --format '{{.Names}}' | grep -qi zookeeper"

# ── Topics ────────────────────────────────────────────────────────────────────
echo ""
echo "[ Topics ]"
check "All 7 topics exist" \
  "[ \$(docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list | grep -v '^__' | wc -l) -ge 7 ]"

check "raw-telemetry has 20 partitions" \
  "docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic raw-telemetry | grep -q 'PartitionCount: 20'"

check "All topics have ReplicationFactor=3" \
  "[ \$(docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe | grep -v '^__' | grep 'ReplicationFactor' | grep -v 'ReplicationFactor: 3' | wc -l) -eq 0 ]"

check "race-outcomes uses compact cleanup policy" \
  "docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic race-outcomes | grep -q 'cleanup.policy=compact'"

check "circuit-metadata uses compact cleanup policy" \
  "docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic circuit-metadata | grep -q 'cleanup.policy=compact'"

# ── Schema Registry ───────────────────────────────────────────────────────────
echo ""
echo "[ Schema Registry ]"
check "Schema Registry HTTP 200 on /subjects" \
  "curl -sf http://localhost:${SCHEMA_REGISTRY_PORT:-8081}/subjects"

check "Schema Registry BACKWARD compatibility" \
  "curl -sf http://localhost:${SCHEMA_REGISTRY_PORT:-8081}/config | grep -q BACKWARD"

# ── Observability ─────────────────────────────────────────────────────────────
echo ""
echo "[ Observability ]"
check "Prometheus is accessible" \
  "curl -sf http://localhost:${PROMETHEUS_PORT:-9090}/-/healthy"

check "Grafana is accessible" \
  "curl -sf http://localhost:${GRAFANA_PORT:-3000}/api/health | grep -q 'ok'"

# ── Phase 3: CDC Enrichment ───────────────────────────────────────────────────
echo ""
echo "[ Phase 3 — Kafka Connect & CDC ]"

check "Kafka Connect cluster healthy" \
  "curl -sf http://localhost:${KAFKA_CONNECT_PORT:-8083}/"

check "Circuits CDC connector RUNNING" \
  "connect_connector_and_tasks_running aerostream-circuits-connector"

check "Drivers CDC connector RUNNING" \
  "connect_connector_and_tasks_running aerostream-drivers-connector"

check "circuit-metadata topic has data" \
  "topic_has_records circuit-metadata"

check "driver-profiles topic has data" \
  "topic_has_records driver-profiles"

check "stream-processor health UP" \
  "stream_processor_health_up"

# ── Phase 4: ksqlDB ───────────────────────────────────────────────────────────
echo ""
echo "[ Phase 4 — ksqlDB ]"

check "ksqlDB REST API healthy" \
  "curl -sf http://localhost:${KSQL_PORT:-8088}/info"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
printf "  Results: %d passed, %d failed\n" $PASS $FAIL
echo "======================================================"

if [ $FAIL -eq 0 ]; then
  echo "  ALL CHECKS PASSED — stack healthy through Phase 4 (ksqlDB)."
  echo ""
  exit 0
else
  echo "  $FAIL CHECK(S) FAILED — fix issues above before proceeding."
  echo ""
  exit 1
fi
