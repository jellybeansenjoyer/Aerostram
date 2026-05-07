# AeroStream

Real-time Formula 1 telemetry processing platform built on Apache Kafka (KRaft), Kafka Streams, and a Python ML inference consumer.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 2 — Telemetry Producer → raw-telemetry (Avro)                        │
└─────────────────────────┬───────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Kafka Cluster (KRaft) · Schema Registry                                    │
│  Topics: raw · enriched · stream-aggregates · circuit-metadata · drivers …  │
└─────────┬───────────────────────────────────────────────────┬───────────────┘
          │ Phase 3 (Kafka Streams + Debezium CDC)               │ Phase 4 ksqlDB
┌─────────▼───────────────────────────────┐   ┌─────────────────▼───────────────┐
│  stream-processor · Postgres reference │   │  hopping windows → JSON sink │
│  → enriched-telemetry                   │   │  → stream-aggregates         │
└─────────────────────────────────────────┘   └───────────────────────────────┘
                                                          │
                                               Phase 5 · ml-consumer (pit ML)
                Observability: Prometheus + Grafana + JMX Exporter
```

## Phases

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | Infrastructure Foundation (Kafka, Schema Registry, Prometheus, Grafana) | Done |
| 2 | Telemetry Simulator (Spring Boot producer, Avro schema) | Done |
| 3 | Stream Enrichment (Kafka Streams, Debezium CDC, PostgreSQL) | Done |
| 4 | ksqlDB Analytics (CSAS / CTAS, hopping-window aggregates → `stream-aggregates`) | Done |
| 5 | ML Inference Consumer (Python, RandomForest, pit-stop predictions) | Pending |

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env

# 2. Generate KRaft Cluster ID (requires Docker)
bash infra/scripts/init-kafka-storage.sh

# 3. Start the full stack
docker compose up -d

# 4. Create all Kafka topics
bash infra/scripts/create-topics.sh

# 5. Set Schema Registry compatibility
bash infra/scripts/configure-schema-registry.sh

# 6. Validate cluster (Phases 1–4 health checks)
bash infra/scripts/validate-cluster.sh

# 7. Phase 4 — deploy ksqlDB queries (after producer + stream-processor have run once so
#    enriched-telemetry value schema exists in Schema Registry)
bash infra/scripts/deploy-ksql-queries.sh
```

## Service Endpoints (default)

| Service | URL |
|---------|-----|
| Kafka Broker 1 | localhost:9092 |
| Kafka Broker 2 | localhost:9094 |
| Kafka Broker 3 | localhost:9096 |
| Schema Registry | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| PostgreSQL | localhost:5432 |
| ksqlDB | http://localhost:8088 |

## Kafka Topics

| Topic | Partitions | RF | Retention | Notes |
|-------|-----------|-----|-----------|-------|
| raw-telemetry | 20 | 3 | 2h | Phase 2 producer output |
| enriched-telemetry | 20 | 3 | 6h | Phase 3 Kafka Streams output |
| stream-aggregates | 10 | 3 | 24h | Phase 4 ksqlDB `AGGREGATE_METRICS` sink (JSON) |
| pit-predictions | 5 | 3 | 24h | Phase 5 ML consumer output |
| race-outcomes | 5 | 3 | compact | Final race results |
| dlq-telemetry | 3 | 3 | 7d | Dead letter queue |
| circuit-metadata | 3 | 3 | compact | Circuit reference data |

## Context & Progress Tracking

See [`context.json`](./context.json) for the current build status and documentation of each completed issue.

**Phase guides:** [Phase 1](./docs/phase-1-infrastructure.md) · [Phase 2](./docs/phase-2-telemetry-producer.md) · [Phase 3](./docs/phase-3-stream-enrichment.md) · [Phase 4](./docs/phase-4-ksqldb-analytics.md)
