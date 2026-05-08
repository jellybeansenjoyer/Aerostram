#!/usr/bin/env bash
# Run cluster validation (Phases 1–5). Expect producer + stream-processor (+data plane) as needed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO}"

bash infra/scripts/validate-cluster.sh
