#!/usr/bin/env bash
# Deploy ksqlDB queries (Phase 4). Run after enriched-telemetry has registered in Schema Registry.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO}"

bash infra/scripts/deploy-ksql-queries.sh
