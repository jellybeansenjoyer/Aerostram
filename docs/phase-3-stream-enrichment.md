# Phase 3 — Stream Enrichment (CDC + Kafka Streams)

**Status:** Complete  
**Total estimate:** ~12 hours  
**Issues covered:** ENRICH-1 through ENRICH-6  
**Stack:** PostgreSQL 16 · Debezium 2.7.0.Final · Kafka Connect 7.6.1 · Spring Boot 3.2.5 · Kafka Streams · Confluent Avro Serde 7.6.1 · Apache Avro 1.11.3

---

## Table of Contents

1. [What Phase 3 Delivers](#what-phase-3-delivers)
2. [Why This Phase Exists](#why-this-phase-exists)
3. [File Tree: New and Modified](#file-tree-new-and-modified)
4. [ENRICH-1 — PostgreSQL, WAL, and F1 Reference Data](#enrich-1--postgresql-wal-and-f1-reference-data)
5. [ENRICH-2 — Kafka Connect and the Debezium Plugin](#enrich-2--kafka-connect-and-the-debezium-plugin)
6. [ENRICH-3 — CDC Connectors and the SMT Chain](#enrich-3--cdc-connectors-and-the-smt-chain)
7. [ENRICH-4 — stream-processor Module and Avro Schemas](#enrich-4--stream-processor-module-and-avro-schemas)
8. [ENRICH-5 — Kafka Streams Enrichment Topology](#enrich-5--kafka-streams-enrichment-topology)
9. [ENRICH-6 — Validation and `validate-cluster.sh`](#enrich-6--validation-and-validate-clustersh)
10. [End-to-End Data Flow](#end-to-end-data-flow)
11. [Startup Sequence](#startup-sequence)
12. [Verification Commands](#verification-commands)
13. [Definition of Done](#definition-of-done)
14. [Key Design Decisions](#key-design-decisions)
15. [Troubleshooting](#troubleshooting)

---

## What Phase 3 Delivers

Phase 1 gave you brokers, Schema Registry, and topics. Phase 2 produced high-rate **raw telemetry** (`TelemetryEvent` on `raw-telemetry`). Phase 3 **joins that live stream with slowly changing reference data** so every event can carry human-readable circuit and driver context — without hard-coding that data inside the producer.

By the end of Phase 3 you have:

- **PostgreSQL 16** as the system of record for circuits, teams, drivers, and season regulations — seeded with F1 2024–style data aligned with Phase 2 (`DRV_01`…`DRV_20`, circuit IDs that match `session_id`).
- **Logical replication (WAL)** enabled on Postgres so **Debezium** can read row-level changes and publish them to Kafka.
- **Kafka Connect** in distributed mode with the **Debezium PostgreSQL connector** loaded via a **volume-mounted plugin directory** (no custom Connect image build).
- **Two CDC connectors** that stream `circuits` → `circuit-metadata` and `drivers` → `driver-profiles`, each with a **three-step Single Message Transform (SMT) chain** so keys are plain strings and values are **Avro** registered in Schema Registry.
- A **`stream-processor` Spring Boot service** running **Kafka Streams** with **two `GlobalKTable` lookups** and **`leftJoin`** semantics, writing **`EnrichedTelemetryEvent`** records to **`enriched-telemetry`**.
- **Automated checks** in `validate-cluster.sh` for Connect health, connector RUNNING state, topic population, and stream-processor health.
- **Prometheus scrape jobs** for Kafka Connect and the stream-processor Actuator endpoint (Grafana dashboards for Phase 3 can be added later; metrics endpoints are ready).

---

## Why This Phase Exists

**Problem:** Telemetry events carry `car_id`, `driver_id`, and `session_id`, but analysts and downstream ML features want **circuit name**, **country**, **DRS zones**, **driver full name**, **team**, and so on. Duplicating that static data inside every event at the producer wastes bandwidth and creates drift when reference data changes.

**Approach:** Treat reference data as **relational data in Postgres**, expose changes to Kafka via **CDC**, and let **Kafka Streams** join the fast telemetry stream with the **latest** reference snapshots in near real time. When you `UPDATE circuits` in SQL, the next telemetry events can reflect the new values after CDC propagates — without redeploying the producer.

**Why not REST enrichment inside the producer?** A database call per event does not scale to tens of thousands of events per second. **Pre-materialized reference data in Kafka**, joined locally inside each stream task, scales horizontally with partitions and keeps the hot path in memory (RocksDB state stores).

---

## File Tree: New and Modified

### New directories and files

```
infra/postgres/init/
├── 01-schema.sql              ← DDL: circuits, teams, drivers, regulations
├── 02-seed-circuits.sql       ← 10 circuits (MONZA, SILVERSTONE, …)
├── 03-seed-teams.sql          ← 10 teams (REDBULL, FERRARI, …)
├── 04-seed-drivers.sql        ← 20 drivers DRV_01..DRV_20
├── 05-regulations.sql         ← F1_2024 regulations row
└── 06-permissions.sql         ← REPLICATION grant for aerostream user

infra/kafka-connect/
├── connectors/
│   ├── aerostream-circuits-connector.json   ← Debezium: public.circuits → circuit-metadata
│   └── aerostream-drivers-connector.json    ← Debezium: public.drivers → driver-profiles
└── plugins/                                 ← populated by download script (gitignored JARs typical)

infra/scripts/
├── download-connect-plugins.sh   ← curls Debezium PostgreSQL plugin tarball, extracts
└── deploy-connectors.sh          ← waits for Connect REST API, POSTs connector JSONs

stream-processor/
├── Dockerfile
├── pom.xml
└── src/
    ├── main/
    │   ├── avro/
    │   │   ├── TelemetryEvent.avsc          ← copy of producer schema (input contract)
    │   │   └── EnrichedTelemetryEvent.avsc  ← output contract: 26 + 12 fields
    │   ├── java/com/aerostream/
    │   │   ├── StreamProcessorApplication.java
    │   │   ├── config/KafkaStreamsConfig.java
    │   │   ├── topology/EnrichmentTopology.java
    │   │   └── enrichment/EnrichmentMapper.java
    │   └── resources/
    │       ├── application.yml
    │       └── application-docker.yml
    └── test/java/com/aerostream/
        └── TopologyTest.java                ← TopologyTestDriver, no real cluster
```

### Modified files

- `docker-compose.yml` — `postgres`, `kafka-connect`, `stream-processor` services; `postgres-data` volume; `kafka-ui` env for Kafka Connect URL.
- `infra/scripts/create-topics.sh` — `driver-profiles`, `connect-configs`, `connect-offsets`, `connect-status`.
- `infra/scripts/validate-cluster.sh` — Phase 3 section (6 checks); success message references Phase 4.
- `infra/prometheus/prometheus.yml` — scrape jobs `kafka-connect`, `aerostream-stream-processor`.
- `.env.example` — `KAFKA_CONNECT_PORT`, `STREAM_PROCESSOR_PORT` (Postgres vars may already exist from earlier phases).

---

## ENRICH-1 — PostgreSQL, WAL, and F1 Reference Data

**Priority:** Critical  
**Purpose:** Give Debezium a real database with logical decoding enabled, and seed rows whose **primary keys align with Phase 2 telemetry** so joins succeed without ad hoc mapping tables.

### Why `wal_level=logical`?

PostgreSQL writes every committed change to the **write-ahead log (WAL)**. By default, replicas consume WAL for physical byte-for-byte copy. **Logical decoding** exposes a **change stream** (row inserts/updates/deletes) that logical replication slots and plugins such as **pgoutput** (used by Debezium) can read. Without `wal_level=logical`, Debezium cannot attach a replication slot and Phase 3 stops at the first connector start.

**Why cap `max_replication_slots` and `max_wal_senders`?** Each Debezium connector typically uses one slot. The canvas specified small limits suitable for dev; production would tune higher with monitoring of WAL retention.

### Docker service (`postgres`)

The Compose service:

- Uses `postgres:16-alpine` for a small image with Postgres 16.
- Sets `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` from env (with defaults for local dev).
- Overrides the server command to append `-c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4`.
- Mounts `./infra/postgres/init` read-only into `/docker-entrypoint-initdb.d` so official Postgres entrypoint runs `.sql` files **once** on first data directory initialization.
- Persists data in named volume `postgres-data`.
- Exposes `5432` (configurable via `POSTGRES_PORT`) and uses `pg_isready` in the healthcheck so downstream services wait for a usable database.

### Init script order (`01` … `06`)

Scripts run in **lexicographic filename order**:

1. **`01-schema.sql`** — Creates `circuits`, `teams`, `drivers`, `regulations`. `drivers.team_id` references `teams(team_id)` so teams must exist before drivers; file order enforces that (`03` teams before `04` drivers).

2. **`02-seed-circuits.sql`** — Inserts 10 rows. **`circuit_id`** values (e.g. `MONZA`, `SILVERSTONE`) are the **same tokens** the stream processor extracts from `session_id` (see ENRICH-5). If these diverged from the simulator’s `session_id` pattern, circuit joins would always miss.

3. **`03-seed-teams.sql`** — 10 constructors; stable `team_id` keys used in `drivers`.

4. **`04-seed-drivers.sql`** — 20 rows with **`driver_id` = `DRV_01` … `DRV_20`**, matching Phase 2’s `TelemetryEvent.driver_id`. Names and teams mirror 2024-style grid data for realism.

5. **`05-regulations.sql`** — Single regulation row `F1_2024` for documentation and future features (not consumed by the current Kafka Streams topology).

6. **`06-permissions.sql`** — `ALTER USER aerostream REPLICATION;` so the Debezium database user can create replication slots.

**Idempotency:** Seed files use `ON CONFLICT … DO NOTHING` where appropriate so re-running scripts against an already-seeded DB (outside Docker init) is safer.

### Topic script update (`driver-profiles`)

`circuit-metadata` already existed from Phase 1. Phase 3 adds **`driver-profiles`**: compacted topic (reference data keyed by driver), **5 partitions**, **RF=3**, matching the canvas so Kafka Streams can scale consumer tasks while keeping driver keys well distributed.

---

## ENRICH-2 — Kafka Connect and the Debezium Plugin

**Priority:** Critical  
**Purpose:** Run **Kafka Connect** as a long-lived worker that loads connector plugins, stores its distributed config in Kafka topics, and exposes a **REST API** for connector lifecycle.

### Why not bake Debezium into a custom Docker image?

Volume-mounting `./infra/kafka-connect/plugins` into `/usr/share/java/kafka-connect-plugins`:

- Avoids maintaining a derived Dockerfile and registry image.
- Lets you upgrade the plugin by re-running the download script and restarting Connect.
- Keeps CI and laptops consistent: same tarball URL, same layout.

### `download-connect-plugins.sh`

The script:

- Defines `DEBEZIUM_VERSION=2.7.0.Final` and the Maven Central URL for the **plugin tarball** (not a single JAR — the archive expands to a directory with all transitive connector JARs).
- Creates `infra/kafka-connect/plugins` and extracts the tarball there.
- Skips download if `debezium-connector-postgres/` already exists (fast repeat runs).

You run this **before** `docker compose up kafka-connect` the first time, or Connect will start without the Postgres connector class.

### Connect worker configuration (high level)

Important environment variables in Compose:

- **`CONNECT_BOOTSTRAP_SERVERS`** — all three internal broker endpoints for fault tolerance.
- **`CONNECT_GROUP_ID`** — `aerostream-connect-cluster`; all Connect workers with the same group share work in distributed mode (here: single worker).
- **Config / offset / status topics** — `connect-configs`, `connect-offsets`, `connect-status` with **replication factor 3** so Connect metadata survives a single broker loss (aligned with a 3-broker cluster).
- **External converters** — `CONNECT_KEY_CONVERTER` and `CONNECT_VALUE_CONVERTER` set to **Confluent `AvroConverter`** with `…_SCHEMA_REGISTRY_URL` so connector **output** keys and values are Avro-typed in Schema Registry (connector-internal traffic still uses JSON converters as specified).
- **`CONNECT_PLUGIN_PATH`** — includes the mounted path so the Debezium JARs are discoverable.
- **`depends_on`** with health conditions on Kafka, Schema Registry, and Postgres so Connect does not start before its dependencies are ready.

### Internal Connect topics

Created by `create-topics.sh`:

- **`connect-configs`** — 1 partition, compacted (connector definitions).
- **`connect-offsets`** — 25 partitions, compacted (high cardinality of source offsets).
- **`connect-status`** — 5 partitions, compacted (task status).

These names must match `CONNECT_*_STORAGE_TOPIC` exactly.

### Kafka UI integration

Two environment variables point Provectus Kafka UI at Connect’s REST URL inside the Docker network so you can inspect connectors, tasks, and errors from the browser.

### Prometheus

A scrape job hits `kafka-connect:8083` on `/metrics` so Connect JVM and worker metrics are available alongside brokers and apps.

---

## ENRICH-3 — CDC Connectors and the SMT Chain

**Priority:** Critical  
**Purpose:** Stream **row-level changes** from Postgres into Kafka topics that Kafka Streams can treat as **changelog-backed tables**, with **keys suitable for joins**.

### Why two connectors instead of one?

Debezium maps tables to topics using naming rules; splitting **circuits** and **drivers** into **separate connectors**:

- Uses **separate replication slots** (`aerostream_circuits_slot`, `aerostream_drivers_slot`) — avoids a single slot becoming a bottleneck and simplifies resetting one pipeline without touching the other.
- Lets you scale or pause CDC for one table independently in operations.

Each connector sets `table.include.list` to a **single** `public.<table>` and uses **filtered** publication auto-create so pgoutput only decodes relevant tables.

### Snapshot mode `initial`

On first start, Debezium performs a **consistent snapshot** of existing rows so `circuit-metadata` and `driver-profiles` are populated **before** any new WAL events. After that, **incremental** changes follow. This is why GlobalKTables in Kafka Streams quickly have 10 + 20 reference records in dev.

### The three SMTs in order (`unwrap` → `route` → `extractKey`)

Debezium’s default Kafka Connect record has a **complex key** (often a struct containing table and primary key). Kafka Streams `GlobalKTable<String, …>` expects a **String** key matching your join key. The SMT chain fixes that.

1. **`ExtractNewRecordState` (alias `unwrap`)**  
   - **What:** Strips the Debezium envelope (`before`, `after`, `op`, `source`, …) and emits the **flattened row** as the value.  
   - **Why:** Downstream transforms and serializers see a normal “row” payload, not nested CDC JSON.  
   - **`handle.deletes=rewrite`:** Deletes become records with null value semantics appropriate for compacted topics (behavior tuned for teaching/demo; tombstones are not dropped because `drop.tombstones=false`).

2. **`RegexRouter` (alias `route`)**  
   - **What:** Rewrites the **topic name** to a fixed string: `circuit-metadata` or `driver-profiles`.  
   - **Why:** Default Debezium topic names are often long prefixes plus schema and table; the router forces **stable, short topic names** that match Phase 1 topic provisioning and Streams config.

3. **`ExtractField$Key` (alias `extractKey`)**  
   - **What:** Replaces the Connect **key** with a **single field** from the key struct — `circuit_id` or `driver_id`.  
   - **Why:** After unwrap, the key may still be a struct; this step yields a **plain string** key (`MONZA`, `DRV_01`) so `Consumed.with(Serdes.String(), …)` and join key selectors align with **no Avro-specific key serde** on the changelog topics.

### Converters on the connector

- **`StringConverter` for keys** — matches string keys post-`ExtractField$Key`.
- **`AvroConverter` for values** — registers/uses schemas in Schema Registry; enables **GenericRecord** consumption in Streams for evolving Debezium-generated schemas without regenerating Java classes for every DDL tweak.

### `deploy-connectors.sh`

- Sources **`${REPO_ROOT}/.env`** when present so **`POSTGRES_PASSWORD`** matches the **`postgres` service** in Docker Compose (same variable both places).
- Builds each POST body by setting **`database.password`** from that env (default **`aerostream_secret`** if unset — same as Compose’s default).
- Polls `GET /connectors` until Connect responds.
- **POST**s each merged payload to `/connectors`.
- Treats **201 Created** and **409 Conflict** as success (idempotent deploy from laptops and CI).
- Waits 15 seconds, then prints connector **RUNNING** state from `/status` JSON via small Python one-liners.

**Password mismatch:** If Connect returns `password authentication failed for user "aerostream"`, **`POSTGRES_PASSWORD` in `.env` must match the password Postgres was created with** on first volume init; editing `.env` alone does not rotate an existing data directory. Align the password or remove the **`postgres-data`** volume and bring Postgres up again.

**Security note:** For production, use **Kafka Connect secrets providers** or externalized secrets instead of embedding passwords in REST payloads.

---

## ENRICH-4 — stream-processor Module and Avro Schemas

**Priority:** High  
**Purpose:** Provide a dedicated JVM service for **stateful stream processing** — separate lifecycle, scaling, and failure domain from the producer.

### `pom.xml` essentials

- Spring Boot **3.2.5** parent (same line as producer for consistency).
- **`spring-kafka`** — brings Spring’s `KafkaStreamsConfiguration` integration.
- **`kafka-streams`** — Apache Kafka Streams library.
- **`kafka-streams-avro-serde`** — Confluent’s `SpecificAvroSerde` and `GenericAvroSerde`.
- **`kafka-avro-serializer`** — Schema Registry client support.
- **`avro-maven-plugin`** — generates `TelemetryEvent`, `EnrichedTelemetryEvent`, `TireCompound` Java sources from `.avsc` under `target/generated-sources/avro`.

**Why copy `TelemetryEvent.avsc` into this module?** The stream processor is a **separate Maven artifact**. It must compile and deserialize **input** events without depending on the `producer` JAR. The `EnrichedTelemetryEvent` schema **imports** the same `TireCompound` enum as the producer so compound types stay identical.

### `EnrichedTelemetryEvent.avsc`

- **First 26 fields** — same logical content as `TelemetryEvent` (car, session, lap, speeds, tires, fuel, g-forces, etc.).
- **Next 10 fields** — union `["null", "type"]` with `"default": null` for circuit and driver enrichment (nullable in Avro means “may be absent”).
- **`enrichment_ts_ms`** — `long` with default `0` — wall-clock-ish timestamp when the mapper ran (useful for latency debugging).
- **`enriched`** — `boolean` default `false` — `withCircuit` sets true when a circuit row matched; `withDriver` sets true when a driver row matched (if only driver matched after a missed circuit, the builder path still reflects the latest mapper rules in code — see source for exact flag semantics when debugging).

### Spring configuration

- **`application.yml`** — local development: external broker ports on localhost, `schema.registry.url` on localhost, `application-id: aerostream-enrichment-v1`, `processing.guarantee: exactly_once_v2`, 2 stream threads, 100 ms commit interval, topic names under `kafka.topics.*`.
- **`application-docker.yml`** — overrides bootstrap servers and Schema Registry URL to **Docker DNS names** (`kafka-1`, `schema-registry`).

### Dockerfile

Multi-stage build mirroring the producer: Maven Corretto 21 builder, Corretto 21 Alpine runtime, non-root user, `SPRING_PROFILES_ACTIVE=docker` in the entrypoint.

### Compose service `stream-processor`

Depends on healthy Kafka brokers, Schema Registry, and **Kafka Connect**. Connect is not strictly required for the Streams binary to run, but the dependency ensures **CDC topics are likely populated** before the processor starts joining — reducing the window of “empty GlobalKTable” at startup.

---

## ENRICH-5 — Kafka Streams Enrichment Topology

**Priority:** Critical  
**Purpose:** Continuously read `raw-telemetry`, enrich each event with **circuit** and **driver** dimensions, emit **`EnrichedTelemetryEvent`** to `enriched-telemetry` keyed by **`car_id`** (same key as input stream for partition affinity).

### `KafkaStreamsConfig.java`

- Registers the **default** `KafkaStreamsConfiguration` bean Spring Kafka expects, with:
  - **`processing.guarantee = exactly_once_v2`** — end-to-end semantics tied to broker transactions; no duplicate writes to `enriched-telemetry` on at-least-once failure scenarios that Streams handles (under supported broker/config constraints).
  - **Default key serde = String**, **default value serde = SpecificAvroSerde** — sensible defaults; explicit `Consumed.with` / `Produced.with` still pin per-edge serde where needed.
  - **`schema.registry.url`** in streams properties for Avro serde wiring.
- Exposes three beans:
  - **`SpecificAvroSerde<TelemetryEvent>`** — deserialize input stream values.
  - **`SpecificAvroSerde<EnrichedTelemetryEvent>`** — serialize output stream values.
  - **`GenericAvroSerde`** — deserialize **CDC topic values** without generating specific Java classes for Debezium’s per-table schemas.

### Why `GlobalKTable` instead of `KTable`?

A **`KTable` is partitioned like its source topic** — a join typically requires **co-partitioning** (same key space and partition count) between streams. Reference topics are **small**, **keyed by circuit_id / driver_id**, while telemetry is keyed by **`car_id`**. They are **not** co-partitioned.

A **`GlobalKTable` materializes the entire changelog topic into every stream thread’s local RocksDB state store**. Each task can resolve `MONZA` → circuit row without a shuffle. Cost: more memory and startup replay; benefit: **simple join semantics** and no repartition topic for reference data.

### `EnrichmentTopology.java`

1. **`builder.stream(raw-telemetry, Consumed.with(String, telemetrySerde))`** — input `KStream<String, TelemetryEvent>` keyed by car id.

2. **`builder.globalTable(circuit-metadata, Consumed.with(String, genericSerde))`** — full replica of circuit reference changelog.

3. **`builder.globalTable(driver-profiles, Consumed.with(String, genericSerde))`** — full replica of driver reference changelog.

4. **First `leftJoin(circuitTable, keySelector, EnrichmentMapper::withCircuit)`**  
   - **Key selector:** `extractCircuitId(session_id)` — splits `RACE_2024_MONZA_R1` on `_` and takes **index 2** → `MONZA`.  
   - **Why `leftJoin`:** If the GlobalKTable has not yet loaded, or session_id does not match any circuit, the event **still passes through** with null circuit fields — no dropped telemetry.

5. **Second `leftJoin(driverTable, keySelector, EnrichmentMapper::withDriver)`**  
   - **Key selector:** `enrichedEvent.getDriverId().toString()` — matches `DRV_xx` keys on `driver-profiles`.  
   - **`leftJoin` again** for the same “never drop events” reason.

6. **`fullyEnriched.to(enriched-telemetry, Produced.with(String, enrichedSerde))`** — sink topic uses the same key as input (**car_id**) so **per-car ordering** from Phase 2’s partitioner is preserved on the output topic when partition counts align (both use 20 partitions in the broader design; verify `enriched-telemetry` partition count in `create-topics.sh` for your deployment).

### `EnrichmentMapper.java`

- **`withCircuit`** — Copies every telemetry field into `EnrichedTelemetryEvent.Builder`, sets `enrichment_ts_ms`, sets `enriched` from whether `circuit != null`, then optionally sets circuit nullable fields by reading **column names** from `GenericRecord` (`circuit_name`, `country`, `length_km`, …) matching Postgres/Debezium field names.
- **`withDriver`** — If `driver == null`, returns the incoming enriched event unchanged. Otherwise builds from existing event and sets driver fields from `full_name`, `abbreviated_name`, `team_id`, `nationality`.
- **Helpers `str`, `dbl`, `integer`** — defensive extraction from `GenericRecord` because logical types may arrive as `Utf8`, `Double`, `Integer`, etc.

### `TopologyTest.java`

Uses **`TopologyTestDriver`** and **Mock Schema Registry** (`mock://` URL) so CI can assert:

- A telemetry event with `session_id` containing `MONZA` and `driver_id` `DRV_01` resolves circuit and driver names when reference records are piped into the global table input topics first.
- **GlobalKTable miss** still produces an output record (left join behavior).
- **`extractCircuitId`** parsing edge cases.

No Docker, no real brokers — fast regression tests for topology wiring.

---

## ENRICH-6 — Validation and `validate-cluster.sh`

**Priority:** High  
**Purpose:** Encode Phase 3 acceptance as **scriptable checks** so “green” means Connect + CDC + stream processor are alive and topics contain data.

### Checks added (Phase 3 section)

After Phase 1–2 checks, the script prints `[ Phase 3 — Kafka Connect & CDC ]` and runs:

1. **Kafka Connect REST root** — `curl` to `localhost:${KAFKA_CONNECT_PORT:-8083}/`.
2. **Circuits connector RUNNING** — greps `RUNNING` in status JSON for `aerostream-circuits-connector`.
3. **Drivers connector RUNNING** — same for `aerostream-drivers-connector`.
4. **`circuit-metadata` / `driver-profiles` have data** — **`kafka-get-offsets`** (sum of partition log-end offsets **> 0**); avoids flaky `kafka-console-consumer` timing on compacted Avro topics.
5. **`stream-processor` health** — JSON **`status == UP`** from `/actuator/health` (no `curl -f`, so a non-2xx wrapper does not hide the body).

On full success, the script prints that **Phase 3 is complete** and suggests proceeding to Phase 4.

**Operational note:** For topic checks 4 to pass, connectors must have completed at least snapshot phase; run `deploy-connectors.sh` and allow time before `validate-cluster.sh`.

---

## Appendix — Line-by-Line Reference (configs and code)

This section ties **what** each important block does to **where** it lives in the repo, in the same spirit as the long-form Phase 1 and Phase 2 docs.

### A.1 `docker-compose.yml` — PostgreSQL service

The `postgres` service wires together image, WAL tuning, init scripts, persistence, and health:

```257:285:docker-compose.yml
  # ── Phase 3: PostgreSQL Reference DB ────────────────────────────────────────
  postgres:
    image: postgres:16-alpine
    container_name: aerostream-postgres
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    environment:
      POSTGRES_DB: aerostream
      POSTGRES_USER: aerostream
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-aerostream_secret}
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=4
      - -c
      - max_wal_senders=4
    volumes:
      - ./infra/postgres/init:/docker-entrypoint-initdb.d:ro
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U aerostream -d aerostream"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 15s
    networks:
      - aerostream-network
```

- **`image: postgres:16-alpine`** — Postgres 16 on a small footprint; sufficient for dev CDC workloads.
- **`POSTGRES_*` env** — Official image creates database and role on first init.
- **`command: … -c wal_level=logical`** — Overrides default `replica`; without this, Debezium cannot create logical replication slots.
- **`max_replication_slots` / `max_wal_senders`** — Headroom for two Debezium slots plus margin; prevents running out of slots under defaults.
- **Volume `./infra/postgres/init` → `/docker-entrypoint-initdb.d:ro`** — Runs `01`–`06` SQL files in order exactly **once** when the data directory is empty; `ro` prevents the container from mutating your git-controlled SQL.
- **`postgres-data` named volume** — Survives `docker compose down` without `-v`, so you do not re-seed on every restart.
- **`pg_isready` healthcheck** — Other services use `depends_on: condition: service_healthy` to avoid racing Postgres during init.

### A.2 `docker-compose.yml` — Kafka Connect service

```287:332:docker-compose.yml
  # ── Phase 3: Kafka Connect + Debezium ───────────────────────────────────────
  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.6.1
    container_name: kafka-connect
    ports:
      - "${KAFKA_CONNECT_PORT:-8083}:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
      CONNECT_REST_PORT: 8083
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
      CONNECT_GROUP_ID: aerostream-connect-cluster
      CONNECT_CONFIG_STORAGE_TOPIC: connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_INTERNAL_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_INTERNAL_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components,/usr/share/java/kafka-connect-plugins
      CONNECT_LOG4J_LOGGERS: "org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR"
    volumes:
      - ./infra/kafka-connect/plugins:/usr/share/java/kafka-connect-plugins:ro
    depends_on:
      kafka-1:
        condition: service_healthy
      kafka-2:
        condition: service_healthy
      kafka-3:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 60s
    networks:
      - aerostream-network
```

- **`CONNECT_*_STORAGE_*`** — Topic names **must** match those created in `create-topics.sh`; RF=3 matches a three-broker HA story.
- **Avro converters + Schema Registry URL** — Connector-produced Kafka records get **schema ids** in the value (and key where used), consistent with Phase 2 Telemetry.
- **Internal JSON converters** — Offsets/configs inside Connect’s internal topics stay JSON; only your connector’s **business** records use Avro as configured per connector.
- **`CONNECT_PLUGIN_PATH`** — Includes the bind-mounted folder where `download-connect-plugins.sh` drops Debezium JARs.
- **`curl … /connectors` healthcheck** — REST is up only after the worker can serve API; `start_period: 60s` allows slow plugin scans on first boot.

### A.3 `application.yml` — stream processor

```1:36:stream-processor/src/main/resources/application.yml
spring:
  application:
    name: aerostream-stream-processor
  kafka:
    bootstrap-servers: localhost:9092,localhost:9094,localhost:9096
    streams:
      application-id: aerostream-enrichment-v1
      properties:
        schema.registry.url: http://localhost:8081
        processing.guarantee: exactly_once_v2
        num.stream.threads: 2
        commit.interval.ms: 100

server:
  port: 8091

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,metrics,info
  endpoint:
    health:
      show-details: always

kafka:
  topics:
    raw-telemetry: raw-telemetry
    circuit-metadata: circuit-metadata
    driver-profiles: driver-profiles
    enriched-telemetry: enriched-telemetry

logging:
  level:
    com.aerostream: INFO
    org.apache.kafka.streams: WARN
```

- **`application-id`** — Kafka Streams **changelog topic prefix** and consumer group derivative; changing it creates a **new** state store namespace — use version suffix (`-v1`) when you make incompatible topology changes.
- **`processing.guarantee: exactly_once_v2`** — Maps to `StreamsConfig.EXACTLY_ONCE_V2` in Java config bean; uses transactions for “read-process-write” boundaries.
- **`kafka.topics.*`** — Externalized topic names so tests or other profiles can override without recompiling.

### A.4 Debezium connector configuration (circuits) — field by field

| JSON path | Purpose |
|-----------|---------|
| `connector.class` | `PostgresConnector` — Debezium’s PostgreSQL source implementation. |
| `database.hostname` | `postgres` — Docker DNS name of the DB container. |
| `database.user` / `password` / `dbname` | Credentials and database name (must match `.env` / Compose for dev). |
| `topic.prefix` | Logical prefix for internal Debezium topics; combined with SMT router, user-facing topic is overridden to `circuit-metadata`. |
| `table.include.list` | `public.circuits` only — minimal blast radius for WAL decoding. |
| `plugin.name` | `pgoutput` — native logical replication output plug-in in PostgreSQL 10+. |
| `publication.autocreate.mode` | `filtered` — publication includes only listed tables. |
| `slot.name` | `aerostream_circuits_slot` — unique slot per connector; **do not duplicate** across connectors. |
| `snapshot.mode` | `initial` — emit existing rows on first start. |
| `transforms` | `unwrap,route,extractKey` — order matters: unwrap first (row body), route (topic rename), extractKey (string PK). |
| `key.converter` | `StringConverter` — Kafka key bytes are plain UTF-8 strings after `ExtractField$Key`. |
| `value.converter` | `AvroConverter` + SR URL — values are Confluent wire format with schema id. |

The **drivers** connector is identical in structure except `table.include.list`, `slot.name`, `transforms.extractKey.field` (`driver_id`), and `transforms.route.replacement` (`driver-profiles`).

### A.5 `EnrichmentTopology` — join wiring (code walkthrough)

```43:86:stream-processor/src/main/java/com/aerostream/topology/EnrichmentTopology.java
        // ── Source: raw telemetry events, keyed by car_id ─────────────────────
        KStream<String, TelemetryEvent> rawStream = builder.stream(
            rawTelemetryTopic,
            Consumed.with(Serdes.String(), telemetrySerde)
        );

        // ── GlobalKTable 1: circuit reference data (from Debezium CDC) ─────────
        // Key: circuit_id String (e.g. "MONZA")
        // Replicated to ALL stream thread state stores — no co-partitioning needed
        GlobalKTable<String, GenericRecord> circuitTable = builder.globalTable(
            circuitMetadataTopic,
            Consumed.with(Serdes.String(), genericSerde)
        );

        // ── GlobalKTable 2: driver reference data (from Debezium CDC) ──────────
        // Key: driver_id String (e.g. "DRV_01")
        GlobalKTable<String, GenericRecord> driverTable = builder.globalTable(
            driverProfilesTopic,
            Consumed.with(Serdes.String(), genericSerde)
        );

        // ── Join 1: enrich with circuit data ───────────────────────────────────
        // Key mapper: extract "MONZA" from session_id "RACE_2024_MONZA_R1"
        // leftJoin: events pass through even when circuit GlobalKTable hasn't loaded yet
        KStream<String, EnrichedTelemetryEvent> withCircuit = rawStream.leftJoin(
            circuitTable,
            (carId, event) -> extractCircuitId(event.getSessionId().toString()),
            EnrichmentMapper::withCircuit
        );

        // ── Join 2: enrich with driver data ────────────────────────────────────
        // Key mapper: driver_id is already a clean String in the event
        // leftJoin: events with unknown drivers still pass through (enriched=false for driver)
        KStream<String, EnrichedTelemetryEvent> fullyEnriched = withCircuit.leftJoin(
            driverTable,
            (carId, enrichedEvent) -> enrichedEvent.getDriverId().toString(),
            EnrichmentMapper::withDriver
        );

        // ── Sink: write enriched events to enriched-telemetry ─────────────────
        fullyEnriched.to(
            enrichedTelemetryTopic,
            Produced.with(Serdes.String(), enrichedSerde)
        );
```

- **Line 44–47** — Declares the **telemetry KStream**; `Consumed.with` pins key/value serde **for this source** (defaults in `KafkaStreamsConfig` are not relied on here).
- **Line 52–55** — **Global** changelog consumption for circuits; every thread has a full copy.
- **Line 59–62** — Same for drivers.
- **Line 67–71** — **`leftJoin`** uses a **key selector** that can return `null`; missing keys still forward the left record with `null` right side — critical for startup ordering.
- **Line 76–80** — Second **`leftJoin`** uses `driver_id` from the **already partially enriched** value.
- **Line 83–86** — **`Produced.with`** ensures output values use `SpecificAvroSerde<EnrichedTelemetryEvent>` so Schema Registry gets the output schema.

### A.6 `EnrichmentMapper` — field copy and null safety

```21:74:stream-processor/src/main/java/com/aerostream/enrichment/EnrichmentMapper.java
    public static EnrichedTelemetryEvent withCircuit(TelemetryEvent event, GenericRecord circuit) {
        EnrichedTelemetryEvent.Builder builder = EnrichedTelemetryEvent.newBuilder()
            .setCarId(event.getCarId())
            ...
            .setEnrichmentTsMs(System.currentTimeMillis())
            .setEnriched(circuit != null);

        if (circuit != null) {
            builder.setCircuitName(str(circuit, "circuit_name"))
                   .setCircuitCountry(str(circuit, "country"))
                   ...
        }

        return builder.build();
    }

    public static EnrichedTelemetryEvent withDriver(EnrichedTelemetryEvent event, GenericRecord driver) {
        if (driver == null) return event;

        return EnrichedTelemetryEvent.newBuilder(event)
            .setDriverFullName(str(driver, "full_name"))
            ...
            .setEnriched(true)
            .build();
    }
```

- **Lines 22–50** — **Explicit per-field copy** from `TelemetryEvent` to `EnrichedTelemetryEvent` avoids reflection and keeps compile-time safety if Avro codegen changes.
- **Line 35** — **`TireCompound.valueOf(event.getTireCompound().name())`** — Maps between two generated enum classes that share the same symbol set (same Avro enum in both schemas).
- **Line 50** — **`setEnriched(circuit != null)`** — Immediate signal that circuit lookup hit; may later become `true` again in `withDriver` when driver matches.
- **Lines 52–59** — Circuit columns use **SQL/Debezium names** (`circuit_name`, `country`, …) as `GenericRecord` field lookups.
- **Lines 64–73** — **Early return** on null driver preserves left-join semantics without rebuilding the object unnecessarily.

### A.7 `KafkaStreamsConfig` — beans and Streams defaults

```33:47:stream-processor/src/main/java/com/aerostream/config/KafkaStreamsConfig.java
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG,           applicationId,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName(),
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG,     StreamsConfig.EXACTLY_ONCE_V2,
            StreamsConfig.NUM_STREAM_THREADS_CONFIG,       2,
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,       100,
            "schema.registry.url",                         schemaRegistryUrl
        );
        log.info("Kafka Streams configured: appId={}, bootstrap={}", applicationId, bootstrapServers);
        return new KafkaStreamsConfiguration(props);
    }
```

Serde beans (same file, lines 49–67) configure `SpecificAvroSerde` for `TelemetryEvent` and `EnrichedTelemetryEvent`, and `GenericAvroSerde` for CDC values — each calls `configure(Map.of("schema.registry.url", …), false)` where `false` means **value serde** (not key).

- **`DEFAULT_STREAMS_CONFIG_BEAN_NAME`** — Spring Kafka discovers this as the **primary** Streams configuration; missing this bean name integration breaks auto-start of `KafkaStreams`.
- **`DEFAULT_VALUE_SERDE_CLASS_CONFIG = SpecificAvroSerde`** — Fallback for edges that do not specify serde explicitly (topology still sets explicit serdes on stream/global table for clarity).
- **Three `@Bean` serde methods** — Shared, correctly configured serdes injected into `EnrichmentTopology` — avoids constructing serdes per record.

---

## End-to-End Data Flow

```text
PostgreSQL (circuits, drivers)
        │  WAL logical decoding (pgoutput)
        ▼
Kafka Connect + Debezium connectors
        │  SMT: unwrap → route → extractKey(String PK)
        ▼
Kafka topics: circuit-metadata, driver-profiles  (Avro values, String keys)
        │  GlobalKTable materialization (every Streams thread)
        ▼
Kafka Streams (stream-processor)
        │  leftJoin + leftJoin + map
        ▼
Kafka topic: enriched-telemetry  (EnrichedTelemetryEvent Avro)
        ▲
        │  same cluster
        │
raw-telemetry  (TelemetryEvent Avro from Phase 2 producer)
```

---

## Startup Sequence

Recommended order for a **cold machine**:

1. **Phase 1–2 baseline** — brokers, Schema Registry, topics, optional producer (see Phase 1/2 docs).
2. **`docker compose up -d postgres`** — wait until healthy (init scripts run on first start only).
3. **`bash infra/scripts/create-topics.sh`** — ensures Phase 3 topics exist (safe to re-run).
4. **`bash infra/scripts/download-connect-plugins.sh`** — downloads Debezium if missing.
5. **`docker compose up -d kafka-connect`** — wait until healthy (can take a minute).
6. **`bash infra/scripts/deploy-connectors.sh`** — installs connectors; wait for snapshot.
7. **`mvn clean package -f stream-processor/pom.xml`** — compile and test.
8. **`docker compose build stream-processor && docker compose up -d stream-processor`**.
9. **`curl -X POST http://localhost:8090/api/simulator/start`** — if you want live traffic (producer must be up).
10. **`bash infra/scripts/validate-cluster.sh`** — assert full stack including Phase 3.

---

## Verification Commands

```bash
# Postgres row counts and WAL mode
docker exec aerostream-postgres psql -U aerostream -d aerostream -c "SELECT COUNT(*) FROM circuits;"
docker exec aerostream-postgres psql -U aerostream -d aerostream -c "SELECT COUNT(*) FROM drivers;"
docker exec aerostream-postgres psql -U aerostream -d aerostream -c "SHOW wal_level;"

# Kafka Connect
curl -s http://localhost:8083/ | head
curl -s http://localhost:8083/connector-plugins | grep -i Postgres

# Connector status
curl -s http://localhost:8083/connectors/aerostream-circuits-connector/status | python3 -m json.tool
curl -s http://localhost:8083/connectors/aerostream-drivers-connector/status | python3 -m json.tool

# Stream processor
curl -s http://localhost:8091/actuator/health | python3 -m json.tool

# Unit tests (no Docker)
mvn -q -f stream-processor/pom.xml test
```

---

## Definition of Done

Phase 3 is “done” when:

| Check | Evidence |
|--------|----------|
| Postgres running with seed data | `SELECT COUNT(*)` returns 10 circuits, 20 drivers |
| WAL logical | `SHOW wal_level` → `logical` |
| Connect up | REST `/` returns version JSON |
| Debezium plugin present | `/connector-plugins` lists `PostgresConnector` |
| Connectors RUNNING | `/status` for both connectors |
| Reference topics populated | Kafka UI or console consumer shows messages |
| Enrichment app healthy | `/actuator/health` = UP |
| Automated validation | `validate-cluster.sh` exits 0 |
| Enriched output (manual) | With simulator running, sample `enriched-telemetry` shows non-null circuit and driver fields when keys match |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Postgres as source of truth | Familiar ops model; easy manual edits to demo live CDC (`UPDATE circuits …`). |
| `wal_level=logical` | Required for Debezium pgoutput consumption. |
| Volume-mounted Debezium plugin | Fast iteration; no custom Connect image. |
| Separate connectors + slots | Isolation and simpler operational reset. |
| SMT chain to String keys | Clean `GlobalKTable<String, GenericRecord>` joins without struct keys. |
| `GenericAvroSerde` for CDC | Debezium schemas evolve with table DDL; avoid regenerating Specific classes per column change. |
| `GlobalKTable` + `leftJoin` | Avoid co-partitioning telemetry with reference topics; never drop telemetry on cold start. |
| `extractCircuitId` from `session_id` | Contract between Phase 2 simulator and Phase 3 SQL seeds — must stay aligned. |
| `exactly_once_v2` | Stronger output guarantees for `enriched-telemetry` in failure scenarios (with supported broker setup). |
| `TopologyTestDriver` tests | Cheap CI signal that joins and serde wiring stay correct. |

---

## Troubleshooting

| Symptom | Likely cause | What to try |
|---------|--------------|-------------|
| Connect starts but no `PostgresConnector` in plugins | Plugin tarball not extracted | Run `download-connect-plugins.sh`, verify `infra/kafka-connect/plugins/debezium-connector-postgres/` exists, restart Connect |
| Connector FAILED | Wrong password / DB not reachable / insufficient privileges | `docker compose logs kafka-connect`; verify `POSTGRES_PASSWORD` matches connector JSON and `.env` |
| **`deploy-connectors.sh` HTTP 400**, message contains **password authentication failed** | `.env` password ≠ Postgres role password (volume initialized earlier with another value), or **Windows CRLF** on `POSTGRES_PASSWORD` line (JDBC sends extra `\r` — **fixed** in script via strip) | Re-run deploy after script update. If it persists: `ALTER USER aerostream PASSWORD '…'` to match `.env`, or reset `postgres-data` volume (see script error text). |
| Task FAILED, trace contains **Connection refused** to Postgres (`postgres` / `aerostream-postgres`) | **Postgres container not running** or crashed (Connect keeps running) | `docker compose ps postgres` → `docker compose up -d postgres`; wait until healthy (`pg_isready`). Then **`docker compose restart kafka-connect`** (or `bash infra/scripts/deploy-connectors.sh`) so Debezium reconnects. |
| Empty `circuit-metadata` | Snapshot not finished or table not in publication | Check connector status task trace; verify `table.include.list` |
| Streams app fails on deserialization | Schema mismatch or wrong serde | Confirm Schema Registry subjects; compare Avro field names in DB columns vs mapper expectations |
| Join always null for circuit | `session_id` pattern drift | Compare simulator `session_id` format with `extractCircuitId` and `circuits.circuit_id` values |
| `validate-cluster.sh` grep checks fail | Binary Avro in console output | Script uses byte-count smoke test; ensure `kafka-console-consumer` (no `.sh`) matches cp-kafka image |

---

*This document describes the Phase 3 implementation in the AeroStream repository. For machine-readable status and per-issue notes, see `context.json` under `phases.phase_3`.*
