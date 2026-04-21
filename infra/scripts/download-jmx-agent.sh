#!/bin/bash
set -e
# Downloads the JMX Prometheus Java Agent jar needed by Kafka brokers.
# Must be run once before `docker compose up`.

JAR_VERSION="0.19.0"
JAR_NAME="jmx_prometheus_javaagent-${JAR_VERSION}.jar"
DEST="infra/prometheus/jmx-config/${JAR_NAME}"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JAR_VERSION}/${JAR_NAME}"

if [ -f "$DEST" ]; then
  echo "JMX agent already present: $DEST"
  exit 0
fi

echo "Downloading JMX Prometheus agent ${JAR_VERSION}..."
curl -L -o "$DEST" "$URL"
echo "Downloaded: $DEST ($(du -sh "$DEST" | cut -f1))"
