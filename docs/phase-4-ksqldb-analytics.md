# Phase 4 — ksqlDB Analytics (Windowed Aggregates)

**Status:** Complete  
**Total estimate:** ~8 hours  
**Issues covered:** AGGR-1 through AGGR-5  
**Stack:** Confluent ksqlDB 7.6.1 · Kafka 7.6.1 · Schema Registry 7.6.1 · Docker Compose

---

## Table of Contents

1. [Who This Doc Is For](#who-this-doc-is-for)
2. [What Phase 4 Delivers](#what-phase-4-delivers)
3. [Concept Primer — Read This Before the Rest](#concept-primer--read-this-before-the-rest)
4. [Why This Phase Exists](#why-this-phase-exists)
5. [How Phase 4 Fits After Phase 3](#how-phase-4-fits-after-phase-3)
6. [File Tree: New and Modified](#file-tree-new-and-modified)
7. [AGGR-1 — ksqlDB Server in Docker Compose](#aggr-1--ksqldb-server-in-docker-compose)
8. [AGGR-2 — Source Stream `ENRICHED_EVENTS`](#aggr-2--source-stream-enriched_events)
9. [AGGR-3 — Aggregate Table `AGGREGATE_METRICS`](#aggr-3--aggregate-table-aggregate_metrics)
10. [AGGR-4 — Deploy and Reset Scripts](#aggr-4--deploy-and-reset-scripts)
11. [AGGR-5 — Topics, Validation, and README](#aggr-5--topics-validation-and-readme)
12. [Line-by-Line SQL Walkthrough](#line-by-line-sql-walkthrough)
13. [End-to-End Data Flow](#end-to-end-data-flow)
14. [Startup Sequence](#startup-sequence)
15. [Verification Commands](#verification-commands)
16. [Definition of Done](#definition-of-done)
17. [Key Design Decisions](#key-design-decisions)
18. [Troubleshooting](#troubleshooting)

---

## Who This Doc Is For

This guide assumes you **fully understand Phases 1–3**: Kafka topics and partitions, Schema Registry and Avro, the telemetry producer, and the stream-processor that writes **`enriched-telemetry`**.

If anything below sounds unfamiliar — **streams vs tables**, **windowed aggregation**, **ksqlDB** — start with the [Concept Primer](#concept-primer--read-this-before-the-rest). No prior SQL-on-streams experience is required.

---

## What Phase 4 Delivers

By the end of Phase 4 you have:

- A **`ksqldb-server` container** (Confluent **cp-ksqldb-server** 7.6.1) on the same Docker network as Kafka and Schema Registry, exposing a **REST API** (default host port **8088**) used to run ksqlDB statements from scripts.
- A **ksqlDB stream** named **`ENRICHED_EVENTS`** that is **not a new topic** — it is a **logical view** over the existing Kafka topic **`enriched-telemetry`**, telling ksqlDB how to deserialize keys (`KAFKA` format) and values (**Avro**, via Schema Registry).
- A **persistent, continuously running query** that turns that high-rate stream into **windowed summary statistics per car**: average and max speed, average tire temperature and wear, and event counts — written to the Kafka topic **`stream-aggregates`** as **JSON** rows (easy to inspect and to consume from Phase 5).
- **Automation**: `infra/scripts/deploy-ksql-queries.sh` applies SQL files in order; `infra/scripts/reset-ksql-queries.sh` tears down ksqlDB objects when you need a clean redeploy.
- **Validation**: `infra/scripts/validate-cluster.sh` gained a **Phase 4** section that checks ksqlDB is alive (`GET /info`).
- **Topic ownership clarified**: **`stream-aggregates` is no longer created by `create-topics.sh`** — it is created when ksqlDB runs the aggregate query, with **10 partitions** and **replication factor 3** to match the AeroStream cluster posture.

---

## Concept Primer — Read This Before the Rest

### What is ksqlDB?

**ksqlDB** is Confluent’s **streaming SQL engine** built on top of **Kafka Streams**. You write **SQL-like statements**; underneath, ksqlDB compiles them into a Kafka Streams application that reads and writes Kafka topics.

Think of it as: **“declarative Kafka Streams”** — you describe *what* rolling summaries you want; ksqlDB handles *how* to distribute work, recover state, and write output topics.

### Stream vs table (ksqlDB meanings — slightly different from Postgres)

| ksqlDB concept | Rough intuition | In AeroStream |
|----------------|-----------------|---------------|
| **STREAM** | An **append-only** sequence of events (facts over time). Each row is an incident — e.g. one telemetry sample. | `ENRICHED_EVENTS` wraps **`enriched-telemetry`**: every message is one enriched sample. |
| **TABLE** | A **materialized, updating** view — often keyed, often the result of **aggregations** or **latest value per key**. Output may be backed by a **changelog topic** in Kafka. | `AGGREGATE_METRICS` holds **per-window, per-car** rollups that **change** as new events arrive in each time window. |

So: **Phase 3** gives you a *firehose* of enriched samples. **Phase 4** builds *summaries* suitable for dashboards or downstream ML features without scanning the entire firehose every time.

### What is a “persistent query”?

When you run `CREATE TABLE … AS SELECT …` in ksqlDB, ksqlDB starts a **long-running job** (a **persistent query**) that stays up until you **terminate** it. It continuously reads new data from input topics and emits new rows to the sink.

That is different from **batch SQL** (run once, exit): streaming SQL **never finishes** — it processes **new Kafka records as they arrive**.

### Why windows?

Each telemetry event has **`timestamp_ms`** (event time). **Windowing** groups events into **time buckets** — e.g. “everything between *T* and *T+30s*” — so you can compute **AVG(speed)** per car **inside that bucket**.

Without windows, an aggregate like AVG(speed) would mean “average since the beginning of time” or would require you to manually reset — awkward for live race analytics.

### Hopping windows (what we use)

A **hopping window** has:

- **SIZE** — width of the window (here **30 seconds**).
- **ADVANCE** — how far the window **slides forward** each step (here **10 seconds**).

So windows **overlap**: e.g. [0–30s], [10–40s], [20–50s], …  

That gives **more frequent updates** than a single non-overlapping window — closer in spirit to **“sliding”** monitoring (many teams use hopping as a practical approximation when they want overlapping views without the cost of true per-event sliding windows).

### Why `WHERE enriched = true`?

Phase 3 **leftJoin** deliberately lets events through even when reference data is missing (`enriched=false`). Aggregates for fleet analytics are more trustworthy when **both** circuit and driver lookups succeeded, so we filter to **`enriched = true`** before averaging.

### Why JSON output for `stream-aggregates`?

**Avro** is great for strict contracts (telemetry). **JSON** rows for aggregated KPIs are easy to **eyeball in Kafka UI**, debug, and consume from lightweight Phase 5 tooling without registering another Avro schema for every dashboard tweak. Trade-off: slightly larger messages and less enforcement — acceptable for analytics sink topics.

---

## Why This Phase Exists

**Problem:** `enriched-telemetry` can carry **tens of thousands of events per second**. Downstream consumers (dashboards, ML feature pipelines) often need **rolling pace and tire health metrics per car** — not every raw sample.

**Approach:** Run **continuous, declarative aggregations** in ksqlDB so summaries land in **`stream-aggregates`** with predictable columns (`avg_speed_kph`, `avg_tire_temp_c`, …).

**Why ksqlDB instead of another Kafka Streams JAR?** Phase 3 already uses Java Kafka Streams for enrichment. Phase 4 analytics are **SQL-shaped** and evolve quickly (change window size, add metrics). ksqlDB lets you iterate **without redeploying a Spring Boot service** for every tweak — you edit SQL and redeploy queries via script.

**Why not Postgres batch jobs?** By the time data lands in Postgres you’ve lost **real-time** semantics and added ETL latency. Here aggregates stay **in the Kafka pipeline**.

---

## How Phase 4 Fits After Phase 3

```
Producer (Phase 2)     →  raw-telemetry
stream-processor (3)   →  enriched-telemetry  (Avro, Schema Registry)
ksqlDB (Phase 4)       →  stream-aggregates    (JSON windows per car_id)
Future ML (Phase 5)    ←  can read either enriched firehose or compact aggregates
```

---

## File Tree: New and Modified

### New

```
infra/ksqldb/
├── 01_enriched_source.sql    ← CREATE STREAM ENRICHED_EVENTS …
└── 02_aggregate_metrics.sql  ← CREATE TABLE AGGREGATE_METRICS … AS SELECT … WINDOW …

infra/scripts/
├── deploy-ksql-queries.sh    ← waits for /info, POSTs each .sql to /ksql
└── reset-ksql-queries.sh     ← SHOW QUERIES → TERMINATE → DROP …
```

### Modified

- `docker-compose.yml` — **`ksqldb-server`** service (Phase 4).
- `infra/scripts/create-topics.sh` — **removed** proactive creation of **`stream-aggregates`** (ksqlDB creates it when the CTAS runs).
- `infra/scripts/validate-cluster.sh` — **Phase 4** block (ksqlDB **`/info`**).
- `.env.example` — **`KSQL_PORT=8088`** (optional override).
- `README.md` — phases table, architecture snippet, quick-start step for **`deploy-ksql-queries.sh`**, service endpoint for ksqlDB.

---

## AGGR-1 — ksqlDB Server in Docker Compose

**Purpose:** Run ksqlDB next to your existing brokers so SQL statements compile to Kafka Streams workers inside the container cluster.

### Key environment variables (why they matter)

| Variable | Role |
|----------|------|
| `KSQL_BOOTSTRAP_SERVERS` | Kafka client bootstrap — all three brokers for resilience. |
| `KSQL_LISTENERS` | Binds HTTP REST **inside** the container on port **8088** (what you map to the host). |
| `KSQL_KSQL_SCHEMA_REGISTRY_URL` | Lets ksqlDB fetch **Avro** schemas for `VALUE_FORMAT='AVRO'` on `enriched-telemetry`. Without Schema Registry, Phase 4 cannot deserialize Phase 3 output. |
| `KSQL_KSQL_SERVICE_ID` | Prefix for **internal Kafka topics** ksqlDB creates (`processing`, `commands`, state changelog topics). Keeps AeroStream’s ksql metadata namespaced (`aerostream_`). |
| `KSQL_KSQL_STREAMS_PROCESSING_GUARANTEE` | **`exactly_once_v2`** — aligns with serious stream processing: avoids duplicate publishes on failure scenarios (same family of guarantee as Phase 3 Streams). |
| `KSQL_KSQL_STREAMS_REPLICATION_FACTOR` / `…_TOPIC_REPLICAS` / `…_INTERNAL_TOPIC_REPLICAS` | Ensure ksqlDB’s **internal topics** survive broker loss — **RF=3** matches your three-broker cluster. |

### Healthcheck

`curl -sf http://localhost:8088/info` — returns server metadata JSON when ksqlDB is ready to accept `/ksql` requests.

---

## AGGR-2 — Source Stream `ENRICHED_EVENTS`

**Purpose:** Register the existing **`enriched-telemetry`** topic as a first-class ksqlDB **STREAM** so SQL can `FROM` it.

**Why `CREATE STREAM IF NOT EXISTS`:** Redeploying the deploy script on a cluster that already has the stream shouldn’t fail — `IF NOT EXISTS` makes the first statement idempotent.

**Why `KEY_FORMAT='KAFKA'`:** Phase 3 keys are plain Kafka serializations of **`car_id`** strings — not Avro-wrapped keys.

**Why `VALUE_FORMAT='AVRO'`:** Values are **`EnrichedTelemetryEvent`** registered in Schema Registry.

**Why `TIMESTAMP='timestamp_ms'`:** Tells ksqlDB which field is **event time** for windowing. Without this, ksqlDB would default to **processing time** (when the broker received the record), which skews race analytics under lag or replay.

---

## AGGR-3 — Aggregate Table `AGGREGATE_METRICS`

**Purpose:** Continuously compute **hopping-window** aggregates grouped by **`car_id`**, and sink JSON rows to **`stream-aggregates`**.

**Why a TABLE and not a STREAM:** The query uses **`WINDOW` + `GROUP BY`** — ksqlDB models that result as a **changelog-backed table** (each window/car combination updates as new facts arrive).

**Why `PARTITIONS=10` and `REPLICAS=3`:** Matches AeroStream’s decision that aggregate throughput is lower than raw telemetry but still needs **parallelism** and **fault tolerance**.

**Why filter `enriched = true`:** Drop partially enriched rows so averages aren’t polluted by join misses.

---

## AGGR-4 — Deploy and Reset Scripts

### `deploy-ksql-queries.sh`

1. Optionally loads **`.env`** so **`KSQL_PORT`** overrides work.
2. Waits until **`GET /info`** succeeds.
3. For each **`infra/ksqldb/*.sql`** (sorted by filename), strips `--` comments and **`POST`s** the statement to **`http://localhost:$KSQL_PORT/ksql`** with **`ksql.streams.auto.offset.reset=earliest`** so replay from the start of retained history if you rebuild state.

**Why Python inside the script:** Reliable JSON POST handling and error surfacing vs. fragile shell quoting.

### `reset-ksql-queries.sh`

1. **`SHOW QUERIES`** — finds running persistent query IDs.
2. **`TERMINATE <id>;`** for each — stops the streaming jobs.
3. **`DROP TABLE IF EXISTS AGGREGATE_METRICS;`** — removes the sink registration (topic handling depends on ksqlDB version/settings; run delete-topic manually if you need a hard reset).
4. **`DROP STREAM IF EXISTS ENRICHED_EVENTS;`** — removes the logical stream wrapper (**does not delete** `enriched-telemetry`).

Use this before redeploying SQL when **`CREATE TABLE … AS SELECT`** already ran (CTAS is not idempotent like `CREATE STREAM IF NOT EXISTS`).

---

## AGGR-5 — Topics, Validation, and README

- **`create-topics.sh`**: Removed **`stream-aggregates`** so **ksqlDB** is the component that creates it with the **exact** partition/replica settings declared in SQL — avoids “topic already exists with wrong config” clashes.
- **`validate-cluster.sh`**: Adds **`curl` ksqlDB `/info`** so a full green run proves brokers, Registry, Phase 3 stack, **and** ksqlDB are up.
- **`README.md`**: Documents **Phase 4** as done, adds **ksqlDB** URL, and lists **`deploy-ksql-queries.sh`** after **`validate-cluster.sh`**.

---

## Line-by-Line SQL Walkthrough

### File `infra/ksqldb/01_enriched_source.sql`

| Line | Text | Explanation |
|------|------|----------------|
| 1–3 | `-- Phase 4 …` | Comments only — document prerequisites (Schema Registry must know the Avro schema; usually requires **at least one** message produced so the subject exists — see Troubleshooting). |
| 5 | `CREATE STREAM IF NOT EXISTS ENRICHED_EVENTS` | Declares a ksqlDB stream object named `ENRICHED_EVENTS`. **`IF NOT EXISTS`** makes redeploys safe. |
| 6–9 | `WITH ( … )` | **Connector properties** for this logical stream — maps to underlying Kafka topic settings. |
| 6 | `KAFKA_TOPIC='enriched-telemetry'` | **Do not create a new topic** — attach to the Phase 3 output topic. |
| 7 | `KEY_FORMAT='KAFKA'` | Keys are Kafka’s plain serialization for strings (`car_id`). |
| 8 | `VALUE_FORMAT='AVRO'` | Values use Confluent Avro + Schema Registry wire format. |
| 9 | `TIMESTAMP='timestamp_ms'` | Use the telemetry event’s own clock for windows — **event time**. |

### File `infra/ksqldb/02_aggregate_metrics.sql`

| Line | Text | Explanation |
|------|------|----------------|
| 1–2 | Comments | Describes hopping windows and JSON sink. |
| 4 | `CREATE TABLE AGGREGATE_METRICS` | Starts a **persistent CTAS** (CREATE TABLE AS SELECT) — continuous query. |
| 4–8 | `WITH ( … )` | Sink configuration for the **output Kafka topic**. |
| 5 | `KAFKA_TOPIC='stream-aggregates'` | Output topic name consumed by dashboards / Phase 5. |
| 6 | `VALUE_FORMAT='JSON'` | Human-readable payload; no extra Avro schema registration for these summaries. |
| 7–8 | `PARTITIONS=10`, `REPLICAS=3` | Parallelism + replication for the sink topic (matches cluster RF posture). |
| 9 | `AS` | Begins the SELECT that defines the query. |
| 10–18 | `SELECT …` | Projects one row per **window × car_id** with metrics analysts care about. |
| 11–13 | `WINDOWSTART`, `WINDOWEND` | ksqlDB-provided pseudo-columns — boundaries of the current hop window in milliseconds (aligned to event time when rowtime is correctly set). |
| 14–15 | `AVG` / `MAX` **speed_kph** | Pace snapshot inside the window. |
| 16–17 | Averages of tire temps / wear | Rolls four corners into simple fleet health signals for the window. |
| 18 | `COUNT(*) AS event_count` | How many enriched samples contributed — useful to spot sparse windows. |
| 19 | `FROM ENRICHED_EVENTS` | Input stream defined in file `01`. |
| 20 | `WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)` | Overlapping 30s windows, advancing every 10s — frequent refresh of aggregates. |
| 21 | `WHERE enriched = true` | Only fully enriched rows — see primer. |
| 22 | `GROUP BY car_id` | One aggregate series **per car** inside each window instance. |
| 23 | `EMIT CHANGES` | Required ksqlDB syntax for streaming queries — emit an update whenever the aggregate changes. |

---

## End-to-End Data Flow

1. **Phase 2** writes **`TelemetryEvent`** Avro to **`raw-telemetry`**.
2. **Phase 3** joins with CDC-fed reference topics → **`EnrichedTelemetryEvent`** on **`enriched-telemetry`**.
3. **Phase 4** ksqlDB reads **`enriched-telemetry`**, computes hopping-window KPIs → **`stream-aggregates`** JSON.
4. **Phase 5** (future) may consume **`stream-aggregates`** for lighter-weight ML features or combine with **`enriched-telemetry`** for sequence models.

---

## Startup Sequence

Typical order **after** the stack from Phases 1–3 is running:

1. `docker compose up -d` (includes **`ksqldb-server`**).
2. Ensure **`enriched-telemetry`** has Schema Registry subjects (start **producer** + **stream-processor** briefly).
3. `bash infra/scripts/deploy-ksql-queries.sh`
4. `bash infra/scripts/validate-cluster.sh` — expect Phase 4 **ksqlDB** check to pass.

---

## Verification Commands

```bash
# ksqlDB alive
curl -s http://localhost:8088/info | python3 -m json.tool

# List registered streams/tables (after deploy)
curl -s -X POST http://localhost:8088/ksql \
  -H 'Content-Type: application/json' \
  -d '{"ksql":"SHOW STREAMS;"}' | python3 -m json.tool

curl -s -X POST http://localhost:8088/ksql \
  -H 'Content-Type: application/json' \
  -d '{"ksql":"SHOW TABLES;"}' | python3 -m json.tool

# Persistent queries running
curl -s -X POST http://localhost:8088/ksql \
  -H 'Content-Type: application/json' \
  -d '{"ksql":"SHOW QUERIES;"}' | python3 -m json.tool

# Topic created by CTAS
docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic stream-aggregates
```

---

## Definition of Done

- **`ksqldb-server`** container healthy; **`/info`** returns JSON on **`localhost:${KSQL_PORT:-8088}`**.
- **`deploy-ksql-queries.sh`** completes without HTTP errors from `/ksql`.
- **`SHOW QUERIES`** lists at least one **RUNNING** persistent query for **`AGGREGATE_METRICS`**.
- **`stream-aggregates`** exists with **10 partitions**, **RF=3**, and receives JSON rows when telemetry flows.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Hopping **30s / 10s** | Balance between freshness and stability of AVG/MAX — tunable without Java deploys. |
| Sink **JSON** | Easier debugging and flexible downstream consumption vs registering another Avro schema for every metric tweak. |
| **`stream-aggregates` owned by ksqlDB** | Avoids conflicting pre-created topic configs from `create-topics.sh`. |
| **Filter `enriched = true`** | Keeps aggregates aligned with successful Phase 3 lookups only. |
| **`exactly_once_v2` ksql setting** | Consistent processing semantics with the rest of the streaming stack. |

---

## Troubleshooting

| Symptom | Likely cause | What to do |
|---------|----------------|------------|
| `deploy-ksql-queries.sh` fails on **01_enriched_source.sql** with schema / deserialization errors | No **`EnrichedTelemetryEvent`** subject yet — topic never received a message | Start **producer** + **stream-processor** once; confirm Schema Registry lists **`EnrichedTelemetryEvent-value`**. |
| **CREATE TABLE …** fails: topic **stream-aggregates** exists with wrong config | Leftover topic from an older run | `bash infra/scripts/reset-ksql-queries.sh`; if needed, manually delete topic via `kafka-topics --delete` **after** understanding data loss implications. |
| **Empty `stream-aggregates`** | No telemetry in **`enriched-telemetry`**, or **`enriched`** always false | Start simulator; verify Phase 3 joins — check sample messages in Kafka UI. |
| High CPU on **`ksqldb-server`** | Large replay (`auto.offset.reset=earliest`) on huge topics | For dev, trim retention on upstream topics or snapshot smaller time ranges (advanced). |
| Cannot redeploy **02** — “already exists” | Previous CTAS still running | Run **`reset-ksql-queries.sh`**, then deploy again. |

---

## Related Documents

- [Phase 1 — Infrastructure](./phase-1-infrastructure.md)
- [Phase 2 — Telemetry Producer](./phase-2-telemetry-producer.md)
- [Phase 3 — Stream Enrichment](./phase-3-stream-enrichment.md)

---

*End of Phase 4 documentation.*
