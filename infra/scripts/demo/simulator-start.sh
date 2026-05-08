#!/usr/bin/env bash
# POST /api/simulator/start on the producer.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

REPO="$(demo_repo_root)"
demo_load_dotenv "${REPO}"

BASE="$(demo_producer_base)"
curl -sS -X POST "${BASE}/api/simulator/start" || {
  echo "WARN: could not reach producer at ${BASE}" >&2
  exit 1
}
echo "Simulator start requested."
