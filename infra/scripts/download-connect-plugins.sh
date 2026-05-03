#!/bin/bash
# Downloads the Debezium PostgreSQL connector plugin and extracts it to the
# volume-mount directory used by Kafka Connect.
# Run BEFORE docker compose up kafka-connect.
#
# Maven coordinates: artifact ID is debezium-connector-postgres (not …postgresql).
# See: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/

set -euo pipefail

PLUGIN_DIR="infra/kafka-connect/plugins"
DEBEZIUM_ARTIFACT="debezium-connector-postgres"
DEBEZIUM_VERSION="2.7.4.Final"
DEBEZIUM_URL="https://repo1.maven.org/maven2/io/debezium/${DEBEZIUM_ARTIFACT}/${DEBEZIUM_VERSION}/${DEBEZIUM_ARTIFACT}-${DEBEZIUM_VERSION}-plugin.tar.gz"
TARGET_DIR="${PLUGIN_DIR}/${DEBEZIUM_ARTIFACT}"

echo "================================================"
echo " AeroStream — Debezium Plugin Downloader"
echo " Artifact: ${DEBEZIUM_ARTIFACT}"
echo " Version: ${DEBEZIUM_VERSION}"
echo "================================================"

if [ -d "${TARGET_DIR}" ]; then
  echo "Plugin already exists at ${TARGET_DIR} — skipping download."
  echo "Delete the directory and re-run to force re-download."
  exit 0
fi

mkdir -p "${PLUGIN_DIR}"

TMP_ARCHIVE="$(mktemp /tmp/debezium-pg.XXXXXX.tar.gz)"
trap 'rm -f "${TMP_ARCHIVE}"' EXIT

echo "Downloading ${DEBEZIUM_URL} ..."
# -f: fail on HTTP 404/5xx (otherwise curl saves an HTML error page and tar breaks)
# -S: show errors; -L: follow redirects
curl -fSL "${DEBEZIUM_URL}" -o "${TMP_ARCHIVE}"

if ! gzip -t "${TMP_ARCHIVE}" 2>/dev/null; then
  echo "ERROR: Download is not a valid gzip file (wrong URL or blocked?). Remove any partial file and retry." >&2
  exit 1
fi

echo "Extracting to ${PLUGIN_DIR}..."
tar -xzf "${TMP_ARCHIVE}" -C "${PLUGIN_DIR}"

echo ""
echo "Plugin directory contents (first few entries):"
ls "${TARGET_DIR}" | head -20
echo ""
echo "Done. Volume-mount path: ./${TARGET_DIR}"
echo "Kafka Connect will pick up the plugin from: /usr/share/java/kafka-connect-plugins"
