#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# AeroStream — Delete messages by removing application Kafka topics and
# recreating them (empty). Also resets Phase 4 ksqlDB queries/tables first so
# stream-aggregates can be removed safely.
#
# Does NOT delete:
#   - __consumer_offsets, __transaction_state, _schemas, etc.
#   - connect-configs / connect-offsets / connect-status (unless CLEAR_CONNECT_INTERNALS=1)
#
# Usage (from repo root):
#   bash infra/scripts/clear-all-topic-data.sh
#
# Optional:
#   CLEAR_CONNECT_INTERNALS=1 bash infra/scripts/clear-all-topic-data.sh
#
# Afterward (if you use ksqlDB aggregates):
#   bash infra/scripts/deploy-ksql-queries.sh
# And restart connectors if you cleared Connect internals:
#   bash infra/scripts/deploy-connectors.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

CONTAINER="${KAFKA_CONTAINER:-kafka-1}"
BROKER="${KAFKA_BOOTSTRAP_INTERNAL:-kafka-1:9092}"

delete_topic() {
  local name="$1"
  if docker exec -e KAFKA_OPTS='' "${CONTAINER}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --delete \
    --topic "${name}" 2>/dev/null; then
    echo "  deleted topic: ${name}"
  else
    echo "  (skip) ${name} — not found or delete failed"
  fi
}

echo ""
echo "══════════════════════════════════════════════════════════════"
echo " AeroStream — clear application Kafka topic data"
echo "══════════════════════════════════════════════════════════════"
echo ""

if ! docker exec -e KAFKA_OPTS='' "${CONTAINER}" kafka-topics --bootstrap-server "${BROKER}" --list &>/dev/null; then
  echo "ERROR: Cannot reach Kafka in container '${CONTAINER}'."
  echo "Start the stack: docker compose up -d"
  exit 1
fi

echo "Step 1 — Reset ksqlDB Phase 4 objects (terminate queries, drop streams/tables)..."
if [[ -x "${SCRIPT_DIR}/reset-ksql-queries.sh" ]]; then
  bash "${SCRIPT_DIR}/reset-ksql-queries.sh" || echo "  (warn) reset-ksql-queries.sh exited non-zero — continuing"
else
  echo "  (skip) reset-ksql-queries.sh not found"
fi

echo ""
echo "Step 2 — Delete application topics (data removed when topic is deleted)..."

# Mirrors infra/scripts/create-topics.sh + Phase 4 sink topic
APP_TOPICS=(
  raw-telemetry
  enriched-telemetry
  pit-predictions
  stream-aggregates
  race-outcomes
  circuit-metadata
  dlq-telemetry
  driver-profiles
)

for t in "${APP_TOPICS[@]}"; do
  delete_topic "${t}"
done

if [[ "${CLEAR_CONNECT_INTERNALS:-0}" == "1" ]]; then
  echo ""
  echo "Step 2b — CLEAR_CONNECT_INTERNALS=1: deleting Kafka Connect internal topics..."
  echo "  WARNING: Connector configs and offsets will be lost."
  for t in connect-configs connect-offsets connect-status; do
    delete_topic "${t}"
  done
fi

echo ""
echo "Step 3 — Recreate empty topics (infra/scripts/create-topics.sh)..."
bash "${SCRIPT_DIR}/create-topics.sh"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo " Done. Topics were recreated empty where deletion succeeded."
echo ""
echo " Next steps:"
echo "   • Redeploy ksql queries:  bash infra/scripts/deploy-ksql-queries.sh"
echo "   • If you cleared Connect internals, redeploy connectors:"
echo "       bash infra/scripts/deploy-connectors.sh"
echo "   • Restart producers/consumers/BFF as needed."
echo "══════════════════════════════════════════════════════════════"
echo ""
