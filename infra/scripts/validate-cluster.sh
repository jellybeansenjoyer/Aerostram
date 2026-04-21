#!/bin/bash
# AeroStream Phase 1 Cluster Validation Suite
# Prints PASS/FAIL for each check. Exits 0 only if all pass.

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

echo ""
echo "======================================================"
echo "  AeroStream — Phase 1 Validation Suite"
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

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
printf "  Results: %d passed, %d failed\n" $PASS $FAIL
echo "======================================================"

if [ $FAIL -eq 0 ]; then
  echo "  ALL CHECKS PASSED — Phase 1 complete. Proceed to Phase 2."
  echo ""
  exit 0
else
  echo "  $FAIL CHECK(S) FAILED — fix issues above before proceeding."
  echo ""
  exit 1
fi
