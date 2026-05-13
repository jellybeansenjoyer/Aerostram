# Kafka Connect, Kafka Streams & Debezium Essentials — Aerostream Edition

> A fundamentals guide tied to this repository: how Connect and Debezium move data from Postgres into Kafka, and how Kafka Streams joins that data with telemetry.
> Concepts are explained first, then mapped to concrete files and settings in Aerostream.

---

## Table of Contents

1. [Kafka Prerequisite — What You Need Before Connect and Streams](#1-kafka-prerequisite)
2. [Kafka Connect — What It Is and Why It Exists](#2-kafka-connect)
3. [Connectors, Tasks, and Workers](#3-connectors-tasks-and-workers)
4. [Distributed Mode and Internal Topics](#4-distributed-mode-and-internal-topics)
5. [Converters — Keys and Values on the Wire](#5-converters)
6. [Single Message Transforms (SMTs)](#6-single-message-transforms-smts)
7. [The Connect REST API](#7-the-connect-rest-api)
8. [Change Data Capture (CDC) — Mental Model](#8-change-data-capture-cdc)
9. [Debezium — How It Fits](#9-debezium)
10. [PostgreSQL + Debezium — WAL, Logical Decoding, Slots](#10-postgresql--debezium)
11. [Aerostream: Connect + Debezium End-to-End](#11-aerostream-connect--debezium-end-to-end)
12. [Kafka Streams — What It Is](#12-kafka-streams)
13. [Topology, KStream, KTable, GlobalKTable](#13-topology-kstream-ktable-globalktable)
14. [Joins in Kafka Streams](#14-joins-in-kafka-streams)
15. [Serdes, Schema Registry, and Generic vs Specific Avro](#15-serdes-schema-registry-and-generic-vs-specific-avro)
16. [Processing Guarantees and `application.id`](#16-processing-guarantees-and-applicationid)
17. [Aerostream: Enrichment Topology Walkthrough](#17-aerostream-enrichment-topology-walkthrough)
18. [Testing Streams Without a Cluster](#18-testing-streams-without-a-cluster)
19. [Future Scope — What Comes Next](#19-future-scope)

---

## 1. Kafka prerequisite

Kafka is a **distributed commit log**: producers append **records** (messages) to **topics**; consumers read them. Each topic is split into **partitions** for parallelism and ordering guarantees (records with the same **key** go to the same partition).

**Why this matters for Connect:** A connector writes to topics. Topic names, partition counts, and compaction policy affect how downstream systems (including Kafka Streams) behave.

**Why this matters for Streams:** A **Kafka Streams application** is implemented as a **consumer group** reading from input topics and writing to output topics. Its `application.id` is the consumer group id.

In Aerostream, topics such as `raw-telemetry`, `circuit-metadata`, `driver-profiles`, and `enriched-telemetry` are created in `infra/scripts/create-topics.sh`. Reference topics use **log compaction** (`cleanup.policy=compact`) so the latest row per key is retained — a natural fit for CDC dimension tables.

---

## 2. Kafka Connect

**Kafka Connect** is a framework for **moving data between Kafka and external systems** without writing custom producer/consumer code for each integration.

Typical patterns:

| Direction | Example |
|-----------|---------|
| **Source** | Database → Kafka (Debezium is a source connector) |
| **Sink** | Kafka → Elasticsearch, S3, JDBC, etc. |

Connect runs as one or more **worker processes**. Each worker loads **connector plugins** (JARs). You define **connector configurations** (often JSON) that tell Connect which plugin to use and how to connect to the source or sink.

In this project, Connect runs in Docker as the `kafka-connect` service (`confluentinc/cp-kafka-connect:7.6.1`). The Debezium PostgreSQL connector is **not** baked into the image; it is downloaded to `infra/kafka-connect/plugins/` and **volume-mounted** into the container so you can upgrade the plugin without rebuilding a custom image.

---

## 3. Connectors, tasks, and workers

- **Connector** — Logical job definition (e.g. “stream `public.circuits` from Postgres to Kafka”). Stored as configuration in Connect’s internal topics.
- **Task** — Actual unit of work. A connector can spawn one or more tasks (`tasks.max`). Debezium Postgres connector typically uses **one task per connector** for a given database capture scope.
- **Worker** — JVM process that executes tasks. In dev you often run a **single worker**; in production you scale workers in the same **Connect cluster** (`CONNECT_GROUP_ID`) so tasks can be rebalanced across machines.

Aerostream sets `"tasks.max": "1"` on both CDC connector JSON files — sufficient for dev reference tables.

---

## 4. Distributed mode and internal topics

Connect can run in **standalone** (single process, file-backed offsets) or **distributed** mode (recommended for production and for anything that must survive restarts cleanly).

In **distributed** mode, workers coordinate via Kafka topics:

| Topic (Aerostream) | Role |
|-------------------|------|
| `connect-configs` | Connector definitions (compacted) |
| `connect-offsets` | Where each connector left off in the source (compacted, higher partition count for many connectors) |
| `connect-status` | Task / connector state |

These names are wired in `docker-compose.yml` under the `kafka-connect` service:

```yaml
CONNECT_CONFIG_STORAGE_TOPIC: connect-configs
CONNECT_OFFSET_STORAGE_TOPIC: connect-offsets
CONNECT_STATUS_STORAGE_TOPIC: connect-status
CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
```

`CONNECT_GROUP_ID: aerostream-connect-cluster` identifies this Connect cluster so multiple workers would share the same group.

`CONNECT_PLUGIN_PATH` lists directories where Connect scans for connector classes. Aerostream adds `/usr/share/java/kafka-connect-plugins` (bind-mounted from `./infra/kafka-connect/plugins`).

---

## 5. Converters

Connect reads/writes **bytes** on Kafka, but connectors deal with **structured objects** internally. **Converters** serialize and deserialize those objects.

Common converters:

| Converter | Typical use |
|-----------|-------------|
| `JsonConverter` | JSON without Schema Registry |
| `StringConverter` | Plain string keys |
| `AvroConverter` (Confluent) | Avro + Schema Registry integration |

**Worker-level defaults** (`CONNECT_KEY_CONVERTER`, `CONNECT_VALUE_CONVERTER`) apply unless overridden in the connector config.

In `docker-compose.yml`, worker defaults use Confluent `AvroConverter` with `CONNECT_*_SCHEMA_REGISTRY_URL` for keys and values. The **connector JSON files** override per-connector:

- **`key.converter`**: `StringConverter` — so the Kafka message key is a plain string (`circuit_id`, `driver_id`), which matches `GlobalKTable<String, …>` in Kafka Streams.
- **`value.converter`**: `AvroConverter` — values are Avro registered in Schema Registry.

**Internal converters** (`CONNECT_INTERNAL_*`) use `JsonConverter` for Connect’s own metadata traffic between components — a common pattern; connector output to Kafka still follows the connector-level converters.

---

## 6. Single Message Transforms (SMTs)

**SMTs** are a chain of small transformations applied to each record **before** it is written to Kafka (sources) or after read (sinks). They are declared in order:

```text
transforms=unwrap,route,extractKey
```

Aerostream’s Debezium connectors use three SMTs (see `infra/kafka-connect/connectors/aerostream-*-connector.json`):

| Order | Alias | Class | Purpose |
|-------|-------|-------|---------|
| 1 | `unwrap` | `io.debezium.transforms.ExtractNewRecordState` | Remove the Debezium **envelope** (`before`, `after`, `op`, `source`, …) and emit the **flattened row** as the value. |
| 2 | `route` | `org.apache.kafka.connect.transforms.RegexRouter` | Rename the target topic to a fixed name (`circuit-metadata` or `driver-profiles`) so topic names stay stable and match `create-topics.sh`. |
| 3 | `extractKey` | `org.apache.kafka.connect.transforms.ExtractField$Key` | Replace the complex Connect key struct with a **single field** (`circuit_id` or `driver_id`) so the Kafka key is a simple type compatible with `String` keys in Streams. |

Other important unwrap settings in this project:

- **`drop.tombstones`**: `false` — tombstones can matter for log compaction semantics.
- **`handle.deletes`**: `rewrite` — delete events are represented in a way the transform chain can handle for downstream consumers.

---

## 7. The Connect REST API

Connect exposes an **HTTP REST API** (default port **8083** in this stack) for lifecycle and inspection:

| Operation | HTTP | Path (conceptually) |
|-----------|------|---------------------|
| List connectors | GET | `/connectors` |
| Create connector | POST | `/connectors` (body = full JSON including `name` and `config`) |
| Get status | GET | `/connectors/{name}/status` |
| Delete connector | DELETE | `/connectors/{name}` |

`infra/scripts/deploy-connectors.sh` waits until `GET /connectors` succeeds, then **POST**s each connector JSON. **201** means created; **409** means already exists (script treats both as success for idempotency).

`infra/scripts/validate-cluster.sh` checks Connect health and greps for `RUNNING` in connector status JSON.

---

## 8. Change Data capture (CDC)

**CDC** means capturing **row-level changes** (insert, update, delete) from a database as they commit, and publishing them to downstream systems — often Kafka — **without** polling `SELECT *` on a schedule.

Benefits:

- Low latency and efficient (only deltas).
- Ordering relative to transaction commit on the source (subject to connector semantics).
- Decouples operational databases from analytics and stream processing.

Debezium implements CDC by reading the database’s **transaction log** (MySQL binlog, Postgres WAL, etc.), not by triggers on every table.

---

## 9. Debezium

**Debezium** is an open-source **CDC platform** implemented as **Kafka Connect source connectors** (one connector type per database).

The **Debezium PostgreSQL connector** (`io.debezium.connector.postgresql.PostgresConnector`):

- Uses a **replication slot** and logical decoding plugin (**pgoutput** is the native Postgres plugin Debezium uses in modern setups).
- Emits change events; with default envelope, values describe `before` / `after` row images and metadata (`op`, `source.ts_ms`, table, etc.).
- Performs an **initial snapshot** of configured tables (unless configured otherwise) so Kafka topics are **backfilled** before streaming incremental changes.

In Aerostream, two **separate** connectors each watch **one table**:

- `public.circuits` → routed topic **`circuit-metadata`**
- `public.drivers` → routed topic **`driver-profiles`**

Splitting connectors **isolates failure domains**, allows **separate replication slots**, and keeps `table.include.list` minimal (important for `publication.autocreate.mode: filtered`).

---

## 10. PostgreSQL + Debezium

### Write-ahead log (WAL)

Postgres appends every change to the **WAL** before commit. Physical replication ships WAL bytes to standbys. **Logical decoding** interprets WAL entries as **logical** row changes — what CDC needs.

### `wal_level=logical`

Required for logical replication / pgoutput-based CDC. Aerostream sets this on the `postgres` service:

```yaml
command:
  - postgres
  - -c
  - wal_level=logical
  - -c
  - max_replication_slots=4
  - -c
  - max_wal_senders=4
```

Without `wal_level=logical`, Debezium cannot create a replication slot and the connector will fail.

### Replication slots

A **slot** is a durable cursor in the logical WAL stream. Each Debezium connector typically holds **one slot** (here: `aerostream_circuits_slot`, `aerostream_drivers_slot`). Slots prevent the server from discarding WAL that has not yet been acknowledged by the consumer — **monitor WAL disk usage** in production.

### Database user permissions

The DB user needs permission to create/use replication slots. Aerostream’s init SQL includes `REPLICATION` privilege for the application user (see `infra/postgres/init/06-permissions.sql` and Phase 3 docs).

### Snapshot mode

`"snapshot.mode": "initial"` means: on first start, take a **consistent snapshot** of included tables, emit those rows to Kafka, then **switch to streaming** WAL changes.

---

## 11. Aerostream: Connect + Debezium end-to-end

### Operational sequence

1. **Create topics** — `bash infra/scripts/create-topics.sh` (includes `circuit-metadata`, `driver-profiles`, Connect internal topics).
2. **Download plugin** — `bash infra/scripts/download-connect-plugins.sh` (Debezium `2.7.0.Final` tarball into `infra/kafka-connect/plugins/`).
3. **Start stack** — `docker compose up -d` (includes `postgres`, `kafka-connect`, etc.).
4. **Deploy connectors** — `bash infra/scripts/deploy-connectors.sh` after Connect is healthy.

### Connector configuration highlights

Both `infra/kafka-connect/connectors/aerostream-circuits-connector.json` and `aerostream-drivers-connector.json` share the same structural pattern:

- **`connector.class`**: `io.debezium.connector.postgresql.PostgresConnector`
- **`database.*`**: host `postgres`, DB `aerostream`, user/password (dev defaults — **not** for production as committed secrets).
- **`topic.prefix`**: `aerostream` — used in internal naming; combined with **RegexRouter** to land on short topic names.
- **`plugin.name`**: `pgoutput`
- **`publication.autocreate.mode`**: `filtered` — publication includes only relevant tables.
- **`slot.name`**: unique per connector.
- **`heartbeat.interval.ms`**: keeps slot activity alive and helps monitor lag.

### Security note (from project practice)

Committed connector JSON leaves **`database.password` empty**; **`deploy-connectors.sh`** sets it from **`POSTGRES_PASSWORD`** (or the compose default). Production should use **Connect secret providers**, **externalized configuration**, or a **secrets manager** — never long-lived credentials in version control.

### Kafka UI

`docker-compose.yml` configures Kafka UI with `KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS` so you can inspect connectors and task errors in the browser.

### Prometheus

`infra/prometheus/prometheus.yml` includes a `kafka-connect` scrape job on **`:7073/metrics`** (JMX javaagent); Connect’s REST port **8083** does not expose Prometheus text at `/metrics`.

---

## 12. Kafka Streams

**Kafka Streams** is a **Java library** (not a separate cluster) for building **stream processing applications**: read from Kafka topics, transform, aggregate, join, and write back to Kafka.

Characteristics:

- Embeds in your JVM (here: Spring Boot **`stream-processor`** module).
- Uses consumer groups under the hood; **state** is stored in local **RocksDB** stores and **changelog topics** in Kafka.
- Supports **exactly-once** processing semantics against Kafka when configured (`processing.guarantee`).

**Contrast with Kafka Connect:**

| | Kafka Connect | Kafka Streams |
|--|---------------|---------------|
| Primary role | Integrate Kafka with external systems | Compute on data already in Kafka |
| Deployment | Connect worker JVMs | Your application JVMs |
| Aerostream use | Debezium: Postgres → Kafka | Join telemetry with CDC topics → `enriched-telemetry` |

---

## 13. Topology, KStream, KTable, GlobalKTable

### Topology

A **topology** is a **DAG** of processors: sources, stateful operators, sinks. You build it with **`StreamsBuilder`** (DSL) or the low-level Processor API.

### KStream

A **KStream** is an **unbounded sequence of records** — typically a changelog-style stream (every event is a fact), e.g. `raw-telemetry`.

### KTable

A **KTable** is a **changelog stream interpreted as a table**: each key keeps the **latest** value. Partitioned like the source topic; **co-partitioning** is required for key-based joins between two KTables or KStream–KTable joins where the join key is the message key.

### GlobalKTable

A **GlobalKTable** is a **replica of the entire topic on every Streams instance** (each thread gets a full copy). **Pros:** join without co-partitioning requirements when the lookup key is **derived from the value** (not the Kafka message key). **Cons:** more memory and startup cost; best for **small reference data**.

Aerostream uses **two `GlobalKTable<String, GenericRecord>`** instances for `circuit-metadata` and `driver-profiles` because:

- Reference data is small (seeded circuits and drivers).
- Join keys are extracted via **key extractors** from telemetry (`session_id` → circuit id, `driver_id` string) — not necessarily the same as the stream’s Kafka key (`car_id`).

Relevant code: `EnrichmentTopology.java` — `builder.globalTable(...)` for both CDC topics.

---

## 14. Joins in Kafka Streams

Aerostream uses **`leftJoin`** twice:

1. **Raw telemetry** `leftJoin` **circuit** `GlobalKTable` — mapper extracts circuit id from `session_id` (e.g. `RACE_2024_MONZA_R1` → `MONZA`).
2. **Result** `leftJoin` **driver** `GlobalKTable` — mapper uses `driver_id` from the event.

**`leftJoin`** means: if there is **no matching row** in the table, the stream event still passes through with **`null`** for the table side — `EnrichmentMapper` handles nulls and sets `enriched` flags accordingly. This is safer than `innerJoin` when reference data might load slowly at startup.

---

## 15. Serdes, Schema Registry, and Generic vs Specific Avro

**Serde** = **Ser**ializer + **De**serializer. Kafka Streams needs serdes for keys and values of each stream/table.

- **`TelemetryEvent`** and **`EnrichedTelemetryEvent`** use **`SpecificAvroSerde`** — generated Java types from `.avsc` schemas (`KafkaStreamsConfig.java` beans `telemetrySerde`, `enrichedSerde`).
- **CDC topic values** (Debezium-generated Avro schemas that evolve with table DDL) use **`GenericAvroSerde`** — records as **`GenericRecord`** with field access by name (`EnrichmentMapper` reads `circuit_name`, `full_name`, etc.).

`KafkaStreamsConfig` also sets:

- **`DEFAULT_KEY_SERDE_CLASS_CONFIG`**: `String`
- **`DEFAULT_VALUE_SERDE_CLASS_CONFIG`**: `SpecificAvroSerde` (default for streams that do not override `Consumed` / `Produced`)
- **`schema.registry.url`** for Confluent integration

`EnrichmentTopology` explicitly uses `Consumed.with(Serdes.String(), telemetrySerde)` for raw telemetry and `Consumed.with(Serdes.String(), genericSerde)` for global tables.

---

## 16. Processing guarantees and `application.id`

### `application.id`

**Unique identifier** for your Streams application — becomes the **consumer group prefix** and part of internal topic names. **Changing it** creates a **new** application with **empty state** — use **versioned ids** (`aerostream-enrichment-v1`) when you need to run a new topology side by side or reset processing.

Configured in `stream-processor/src/main/resources/application.yml`:

```yaml
spring.kafka.streams.application-id: aerostream-enrichment-v1
```

### Processing guarantee

`KafkaStreamsConfig.java` sets:

```java
StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2
```

**Exactly-once v2** (EOS) provides transactional processing against Kafka: under failure and retry, output records are not duplicated in the Kafka log relative to the configured semantics (see Apache Kafka docs for broker version requirements and interactions with source topics).

Other settings in code: **`NUM_STREAM_THREADS_CONFIG`** (`2`), **`COMMIT_INTERVAL_MS_CONFIG`** (`100`) — tune throughput vs. commit overhead.

---

## 17. Aerostream: enrichment topology walkthrough

**Class:** `stream-processor/src/main/java/com/aerostream/topology/EnrichmentTopology.java`

**Bean method:** `enrichedTelemetryKStream(...)` builds:

1. **`KStream<String, TelemetryEvent>`** from `kafka.topics.raw-telemetry`.
2. **`GlobalKTable<String, GenericRecord>`** from `kafka.topics.circuit-metadata`.
3. **`GlobalKTable<String, GenericRecord>`** from `kafka.topics.driver-profiles`.
4. Two **`leftJoin`** steps calling **`EnrichmentMapper.withCircuit`** and **`EnrichmentMapper.withDriver`**.
5. **`to(enriched-telemetry)`** with `Produced.with(Serdes.String(), enrichedSerde)`.

**Helper:** `extractCircuitId(String sessionId)` parses the Phase 2 session id format.

**Spring wiring:** `@Configuration` exposes the topology as a `@Bean` returning `KStream<…>`; `@EnableKafkaStreams` on `StreamProcessorApplication` activates Spring Kafka Streams auto-configuration alongside `KafkaStreamsConfig`.

---

## 18. Testing streams without a cluster

`stream-processor/src/test/java/com/aerostream/TopologyTest.java` uses:

- **`TopologyTestDriver`** — in-memory execution of the topology (no brokers).
- **`MockSchemaRegistry`** — Confluent test utility with `mock://` URL for serde configuration.
- **Reflection** to set `@Value` fields on `EnrichmentTopology` for topic names.

This pattern is ideal for **unit-level** verification of joins and mappers; integration tests would add Testcontainers or an embedded Kafka.

---

## 19. Future scope

Concepts you are likely to need as Aerostream grows:

| Area | Why it matters |
|------|----------------|
| **More tables / connectors** | Additional dimensions (teams, regulations) → new Debezium connectors or expanded `table.include.list` (trade-offs: slot usage, blast radius). |
| **Schema evolution** | DDL changes → Avro schema compatibility in Schema Registry (`BACKWARD`, `FORWARD`, `FULL`). `GenericAvroSerde` eases evolution for CDC payloads. |
| **Connector restarts / offset** | Understanding `connect-offsets` and Debezium snapshot modes for recovery (`initial_only`, `no_data`, etc.). |
| **Sink connectors** | Push `enriched-telemetry` to S3, BigQuery, or another warehouse via Connect sinks — same REST and converter patterns. |
| **Debezium signaling** | Ad hoc snapshots or incremental snapshot control via Kafka topics. |
| **Kafka Streams interactive queries** | Expose local state store via REST for debugging or serving. |
| **Changelog vs. compacted topics** | KTable materialization semantics vs. CDC topic design. |
| **ksqlDB / Flink** | Higher-level SQL or distributed stateful processing if Java topologies become too heavy — different deployment model, similar event-time concepts. |
| **Secrets & ACLs** | Production Connect auth (mTLS, SASL), Postgres least-privilege users, Kafka ACLs for Connect and Streams principals. |
| **Monitoring lag** | Consumer lag for Streams group, Debezium **binlog/WAL lag** metrics, Connect task failures in Kafka UI / Prometheus. |

---

## Quick reference — files in this repo

| Concern | Location |
|---------|----------|
| Connect worker env | `docker-compose.yml` → `kafka-connect` |
| Debezium plugin install | `infra/scripts/download-connect-plugins.sh` |
| Connector definitions | `infra/kafka-connect/connectors/*.json` |
| Deploy connectors | `infra/scripts/deploy-connectors.sh` |
| Connect / CDC topics | `infra/scripts/create-topics.sh` |
| Postgres WAL / logical | `docker-compose.yml` → `postgres` `command:` |
| Streams config + serdes | `stream-processor/.../KafkaStreamsConfig.java` |
| Topology | `stream-processor/.../EnrichmentTopology.java` |
| Row mapping | `stream-processor/.../EnrichmentMapper.java` |
| Topic names (YAML) | `stream-processor/src/main/resources/application.yml` |
| Topology tests | `stream-processor/src/test/.../TopologyTest.java` |
| Deeper design narrative | `docs/phase-3-stream-enrichment.md` |

---

## Mental model — one diagram

```text
PostgreSQL (WAL, logical)
        │
        ▼  Debezium PostgresConnector (Kafka Connect)
        │  SMTs: unwrap → route → extractKey
        ▼
Kafka topics: circuit-metadata, driver-profiles  (Avro values, String keys)
        │                              ▲
        │                              │
raw-telemetry ───────────────► Kafka Streams (enrichmentTopology)
        (TelemetryEvent)              │
                                      │ GlobalKTable joins + EnrichmentMapper
                                      ▼
                         enriched-telemetry (EnrichedTelemetryEvent)
```

This document pairs with **`docs/phase-3-stream-enrichment.md`** for step-by-step phase implementation details; use this file when you want **concept definitions** and **how they map to Aerostream’s code and infra**.
