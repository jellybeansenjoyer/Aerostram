#!/bin/bash
set -e

echo "=================================================="
echo " AeroStream — KRaft Cluster ID Initialiser"
echo "=================================================="

# Ensure .env exists
if [ ! -f .env ]; then
  echo "No .env found. Copying from .env.example..."
  cp .env.example .env
fi

# Check if CLUSTER_ID is already set and non-empty
EXISTING=$(grep "^KAFKA_CLUSTER_ID=" .env | cut -d= -f2)
if [ -n "$EXISTING" ]; then
  echo "KAFKA_CLUSTER_ID is already set: $EXISTING"
  echo "To regenerate, clear the value in .env and re-run this script."
  exit 0
fi

echo "Generating KRaft CLUSTER_ID (requires Docker)..."
CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid 2>/dev/null | tr -d '\n')

if [ -z "$CLUSTER_ID" ]; then
  echo "ERROR: Failed to generate CLUSTER_ID. Is Docker running?"
  exit 1
fi

echo "Generated CLUSTER_ID: $CLUSTER_ID"

# Write to .env
if grep -q "^KAFKA_CLUSTER_ID=" .env; then
  sed -i.bak "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=$CLUSTER_ID|" .env
  rm -f .env.bak
else
  echo "KAFKA_CLUSTER_ID=$CLUSTER_ID" >> .env
fi

echo ""
echo "SUCCESS — KAFKA_CLUSTER_ID written to .env"
echo "Now run: docker compose up -d"
