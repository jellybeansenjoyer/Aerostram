# AeroStream

Real-time Formula 1 telemetry processing platform built on Apache Kafka (KRaft), Kafka Streams, and a Python ML inference consumer.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 2 — Telemetry Producer                                               │
│  Spring Boot · 20 cars · 10 events/sec/car → raw-telemetry (20 partitions) │
└─────────────────────────┬───────────────────────────────────────────────────┘
                          │ Avro / Schema Registry
┌─────────────────────────▼───────────────────────────────────────────────────┐
│  Kafka Cluster  (3-broker KRaft, combined mode — no ZooKeeper)              │
│  Topics: raw-telemetry · enriched-telemetry · stream-aggregates             │
│          pit-predictions · race-outcomes · dlq-telemetry · circuit-metadata │
└───┬───────────────────────────────────────────────────────────────────┬─────┘
    │ Phase 3 — Kafka Streams                                           │ Phase 5
┌───▼────────────────┐                                        ┌────────▼──────┐
│  stream-processor  │ CDC enrichment via Debezium+Postgres   │  ml-consumer  │
│  EnrichedTelemetry │ → enriched-telemetry (20 partitions)   │  Python ML    │
│  TireAggregates    │                                        │  pit-predict  │
└────────────────────┘                                        └───────────────┘
                Observability: Prometheus + Grafana + JMX Exporter
```

## Phases

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | Infrastructure Foundation (Kafka, Schema Registry, Prometheus, Grafana) | 🔨 In Progress |
| 2 | Telemetry Simulator (Spring Boot producer, Avro schema) | ⏳ Pending |
| 3 | Stream Enrichment (Kafka Streams, Debezium CDC, PostgreSQL) | ⏳ Pending |
| 4 | ksqlDB Analytics (CSAS queries, sliding window aggregates) | ⏳ Pending |
| 5 | ML Inference Consumer (Python, RandomForest, pit-stop predictions) | ⏳ Pending |

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

# 6. Validate Phase 1
bash infra/scripts/validate-cluster.sh
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

## Kafka Topics

| Topic | Partitions | RF | Retention | Notes |
|-------|-----------|-----|-----------|-------|
| raw-telemetry | 20 | 3 | 2h | Phase 2 producer output |
| enriched-telemetry | 20 | 3 | 6h | Phase 3 Kafka Streams output |
| stream-aggregates | 10 | 3 | 24h | Tire + pace aggregates |
| pit-predictions | 5 | 3 | 24h | Phase 5 ML consumer output |
| race-outcomes | 5 | 3 | compact | Final race results |
| dlq-telemetry | 3 | 3 | 7d | Dead letter queue |
| circuit-metadata | 3 | 3 | compact | Circuit reference data |

## Context & Progress Tracking

See [`context.json`](./context.json) for the current build status and documentation of each completed issue.
