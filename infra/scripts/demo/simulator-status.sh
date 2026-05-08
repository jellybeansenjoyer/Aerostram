#!/usr/bin/env bash
# GET /api/simulator/status (JSON).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

REPO="$(demo_repo_root)"
demo_load_dotenv "${REPO}"

BASE="$(demo_producer_base)"
curl -sS "${BASE}/api/simulator/status" | python3 -m json.tool 2>/dev/null || curl -sS "${BASE}/api/simulator/status"
