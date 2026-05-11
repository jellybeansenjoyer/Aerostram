#!/usr/bin/env bash
# Regenerate all architecture diagram exports (light, dark, LinkedIn letterboxed dark).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
MMD="$ROOT/docs/diagrams/aerostream-architecture.mmd"
CFG="$ROOT/docs/diagrams/mermaid-config.json"
CFGD="$ROOT/docs/diagrams/mermaid-config-dark.json"
DIAG="$ROOT/docs/diagrams"
MERMAID_VER="${MERMAID_CLI_VERSION:-11.4.0}"

npx --yes "@mermaid-js/mermaid-cli@${MERMAID_VER}" -c "$CFG" -i "$MMD" \
  -o "$DIAG/aerostream-architecture.svg" -w 3200 -H 2800
npx --yes "@mermaid-js/mermaid-cli@${MERMAID_VER}" -c "$CFG" -i "$MMD" \
  -o "$DIAG/aerostream-architecture.png" -w 3200 -H 2800 -s 2

npx --yes "@mermaid-js/mermaid-cli@${MERMAID_VER}" -c "$CFGD" -i "$MMD" \
  -o "$DIAG/aerostream-architecture-dark.svg" -w 3200 -H 2800
npx --yes "@mermaid-js/mermaid-cli@${MERMAID_VER}" -c "$CFGD" -i "$MMD" \
  -o "$DIAG/aerostream-architecture-dark.png" -w 3200 -H 2800 -s 2

VENDOR="$DIAG/.pillow_vendor"
mkdir -p "$VENDOR"
pip3 install --upgrade pillow --target "$VENDOR" -q
PYTHONPATH="$VENDOR" python3 "$DIAG/fit-linkedin.py"
echo "Done. Outputs under $DIAG"
