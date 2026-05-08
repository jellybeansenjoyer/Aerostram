#!/usr/bin/env bash
# POST /api/simulator/stop on the producer (reduces load during inspections).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

REPO="$(demo_repo_root)"
demo_load_dotenv "${REPO}"

BASE="$(demo_producer_base)"
curl -sS -X POST "${BASE}/api/simulator/stop" || {
  echo "WARN: could not reach producer at ${BASE} (is it up?)" >&2
  exit 1
}
echo "Simulator stop requested."
