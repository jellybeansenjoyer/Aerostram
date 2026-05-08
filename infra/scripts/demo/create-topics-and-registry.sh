#!/usr/bin/env bash
# Idempotent topic creation + Schema Registry BACKWARD config (Phase 1 baseline).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO}"

bash infra/scripts/create-topics.sh
bash infra/scripts/configure-schema-registry.sh
