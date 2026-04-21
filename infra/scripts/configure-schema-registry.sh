#!/bin/bash
set -e

SR_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-8081}"

echo "Waiting for Schema Registry at $SR_URL..."
until curl -sf "$SR_URL/subjects" > /dev/null; do
  echo "  not ready yet, retrying in 3s..."
  sleep 3
done

echo "Schema Registry is up. Setting BACKWARD compatibility..."
RESULT=$(curl -s -X PUT "$SR_URL/config" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}')
echo "Response: $RESULT"

VERIFY=$(curl -sf "$SR_URL/config")
if echo "$VERIFY" | grep -q "BACKWARD"; then
  echo "SUCCESS: Global compatibility set to BACKWARD"
else
  echo "ERROR: Failed to set compatibility. Got: $VERIFY"
  exit 1
fi
