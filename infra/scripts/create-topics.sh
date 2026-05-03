#!/bin/bash
set -e

BROKER="kafka-1:9092"
CONTAINER="kafka-1"

echo "=============================================="
echo " AeroStream — Kafka Topic Provisioner"
echo "=============================================="

echo "Waiting for Kafka broker..."
until docker exec -e KAFKA_OPTS='' $CONTAINER kafka-topics --bootstrap-server $BROKER --list &>/dev/null; do
  echo "  broker not ready, retrying in 3s..."
  sleep 3
done
echo "Broker ready. Creating topics..."
echo ""

create_topic() {
  local NAME=$1; shift
  printf "  %-25s" "$NAME"
  docker exec -e KAFKA_OPTS='' $CONTAINER kafka-topics \
    --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic "$NAME" "$@" > /dev/null 2>&1 \
    && echo "CREATED" || echo "EXISTS (skipped)"
}

# ── High-throughput telemetry topics ──────────────────────────────────────────
create_topic raw-telemetry \
  --partitions 20 --replication-factor 3 \
  --config retention.ms=7200000 \
  --config min.insync.replicas=2

create_topic enriched-telemetry \
  --partitions 20 --replication-factor 3 \
  --config retention.ms=21600000 \
  --config min.insync.replicas=2

# ── Aggregate / prediction topics ─────────────────────────────────────────────
create_topic stream-aggregates \
  --partitions 10 --replication-factor 3 \
  --config retention.ms=86400000

create_topic pit-predictions \
  --partitions 5 --replication-factor 3 \
  --config retention.ms=86400000

# ── Compacted reference topics ────────────────────────────────────────────────
create_topic race-outcomes \
  --partitions 5 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1

create_topic circuit-metadata \
  --partitions 3 --replication-factor 3 \
  --config cleanup.policy=compact

# ── Dead letter queue ─────────────────────────────────────────────────────────
create_topic dlq-telemetry \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=604800000

# ── Phase 3: Reference data + Kafka Connect internal topics ───────────────────
create_topic driver-profiles \
  --partitions 5 --replication-factor 3 \
  --config cleanup.policy=compact

create_topic connect-configs \
  --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact

create_topic connect-offsets \
  --partitions 25 --replication-factor 3 \
  --config cleanup.policy=compact

create_topic connect-status \
  --partitions 5 --replication-factor 3 \
  --config cleanup.policy=compact

echo ""
echo "ALL TOPICS CREATED. Current topic list:"
docker exec -e KAFKA_OPTS='' $CONTAINER kafka-topics --bootstrap-server $BROKER --list
