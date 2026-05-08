#!/usr/bin/env bash
# Deploy Debezium CDC connectors (Phase 3). Requires Kafka Connect healthy.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO}"

bash infra/scripts/deploy-connectors.sh
