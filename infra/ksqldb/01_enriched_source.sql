-- Phase 4 — Source stream over Phase 3 enriched Avro topic (Schema Registry).
-- Requires at least one message on enriched-telemetry so value schema can be resolved,
-- OR rely on ksqlDB Schema Registry lookup for EnrichedTelemetryEvent-value.

CREATE STREAM IF NOT EXISTS ENRICHED_EVENTS WITH (
  KAFKA_TOPIC='enriched-telemetry',
  KEY_FORMAT='KAFKA',
  VALUE_FORMAT='AVRO',
  TIMESTAMP='timestamp_ms'
);
