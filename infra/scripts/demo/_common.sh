#!/usr/bin/env bash
# Shell helpers for AeroStream demo / system-test scripts.
# shellcheck shell=bash

demo_repo_root() {
  # Resolves repo root from this file location (infra/scripts/demo → ../../../).
  (cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
}

demo_load_dotenv() {
  local root="$1"
  if [[ -f "${root}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${root}/.env"
    set +a
  fi
}

demo_producer_port() {
  echo "${PRODUCER_PORT:-8090}"
}

demo_producer_base() {
  echo "http://127.0.0.1:$(demo_producer_port)"
}
