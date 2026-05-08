# AeroStream — Demo & System Test Suite (Phases 1–5)

This document is an **end-to-end integration and demo guide**: it ties each phase to **project objectives** (real-time Kafka processing, Schema Registry governance, enrichment, analytics, ML inference), gives **repeatable test cases**, **startup automation**, and **precautions** so high telemetry rates do not overwhelm laptops or obscure failures.

---

## Table of Contents

1. [Project objectives (what “good” looks like)](#1-project-objectives-what-good-looks-like)
2. [How to read a test case](#2-how-to-read-a-test-case)
3. [Global prerequisites](#3-global-prerequisites)
4. [Global precautions (high throughput)](#4-global-precautions-high-throughput)
5. [Script inventory](#5-script-inventory)
6. [Phase 1 — Infrastructure](#6-phase-1--infrastructure)
7. [Phase 2 — Telemetry producer](#7-phase-2--telemetry-producer)
8. [Phase 3 — CDC + stream enrichment](#8-phase-3--cdc--stream-enrichment)
9. [Phase 4 — ksqlDB analytics](#9-phase-4--ksqldb-analytics)
10. [Phase 5 — ML consumer](#10-phase-5--ml-consumer)
11. [Full-stack demo (executive walkthrough)](#11-full-stack-demo-executive-walkthrough)
12. [Optional soak / load characterization](#12-optional-soak--load-characterization)
13. [Post-run cleanup & stabilization](#13-post-run-cleanup--stabilization)
14. [Failure playbook (quick)](#14-failure-playbook-quick)

---

## 1. Project objectives (what “good” looks like)

| Objective | How phases prove it |
|-----------|---------------------|
| **Scalable real-time ingestion** | Phase 2 producer → **`raw-telemetry`** at configurable EPS; partitions aligned with cars. |
| **Kafka platform correctness** | Phase 1: KRaft cluster RF=3, explicit topics, no auto-create surprises. |
| **Contract governance** | Schema Registry **BACKWARD** + Avro on hot paths. |
| **Reference data + stream joins** | Phase 3: Postgres + Debezium → compacted topics; Kafka Streams → **`enriched-telemetry`**. |
| **Streaming analytics** | Phase 4: ksqlDB windowed aggregates → **`stream-aggregates`**. |
| **Downstream intelligence** | Phase 5: ML inference → **`pit-predictions`** without blocking the Java pipeline. |
| **Operability** | Health endpoints, Prometheus scrapes, **`validate-cluster.sh`** gate. |

---

## 2. How to read a test case

Each **TC-P{phase}-{nn}** block includes:

- **Startup (run first)** — scripts or compose commands to reach the right baseline.
- **Steps** — ordered actions.
- **Expected** — pass criteria.
- **Precautions** — throttle or stop traffic **before** operations that are sensitive to volume (UI browsing, manual consume, log inspection).
- **Teardown** — stop simulator or lower rate after the case so the stack stays stable for the next case.

All scripts live under **`infra/scripts/demo/`** and assume you run them from the **repository root** unless noted.

---

## 3. Global prerequisites

1. **Clone + `.env`:** `cp .env.example .env` and set **`KAFKA_CLUSTER_ID`** via `bash infra/scripts/init-kafka-storage.sh`.
2. **JMX agent (Phase 1 broker metrics):** `bash infra/scripts/download-jmx-agent.sh` before first `docker compose up` if not already done.
3. **Docker resources:** assign enough RAM/CPU (often **≥ 8 GB RAM** for full stack). Brokers OOM-killed (exit 137) usually mean allocate more memory.
4. **Ports free:** 9092/9094/9096, 8081, 8080, 8090, 8091, 8083, 8088, 8099, 9090, 3000, 5432 — or override in `.env`.

---

## 4. Global precautions (high throughput)

The producer defaults to a **high event rate** (see `SIMULATOR_EVENTS_PER_SECOND` in `.env` / `docker-compose.yml`). For **functional testing** and **demos**, treat aggressive throughput as optional.

| Risk | Mitigation |
|------|------------|
| Laptop CPU/disk saturation | Lower **`SIMULATOR_EVENTS_PER_SECOND`** (e.g. **50–300**) before starting long sessions; restart **`producer`** after changing env: `docker compose up -d producer`. |
| Impossible to read Kafka UI / logs | **`bash infra/scripts/demo/simulator-stop.sh`** before deep-diving in UI or tailing logs. |
| ML consumer keeps pace but heats CPU | Set **`CONSUMER_SLOWDOWN_MS`** (e.g. **2–20**) and recreate: `CONSUMER_SLOWDOWN_MS=10 docker compose up -d ml-consumer`. |
| Validation / offset checks flaky under flood | Stop simulator **before** `validate-cluster.sh` if you only need health checks; or run validation with **moderate** EPS. |
| ksqlDB / Connect REST timeouts | Reduce EPS temporarily; ensure **three brokers healthy** (not stopped). |

**Conservative one-liners (current shell only):**

```bash
export SIMULATOR_EVENTS_PER_SECOND=150
export CONSUMER_SLOWDOWN_MS=5
docker compose up -d producer ml-consumer
```

---

## 5. Script inventory

| Script | Purpose |
|--------|---------|
| **`infra/scripts/demo/start-stack-through-phase.sh`** `<1\|2\|3\|4\|5>` | Idempotent **cumulative** Compose up through that phase. |
| **`infra/scripts/demo/create-topics-and-registry.sh`** | `create-topics.sh` + `configure-schema-registry.sh`. |
| **`infra/scripts/demo/deploy-phase3-connectors.sh`** | Runs **`deploy-connectors.sh`** (Debezium connectors). |
| **`infra/scripts/demo/deploy-phase4-ksql.sh`** | Runs **`deploy-ksql-queries.sh`**. |
| **`infra/scripts/demo/simulator-stop.sh`** | POST **`/api/simulator/stop`**. |
| **`infra/scripts/demo/simulator-start.sh`** | POST **`/api/simulator/start`**. |
| **`infra/scripts/demo/simulator-status.sh`** | GET **`/api/simulator/status`** (pretty JSON). |
| **`infra/scripts/demo/run-full-validation.sh`** | Runs **`validate-cluster.sh`** (Phases 1–5). |

**Plugins (Phase 3):** before first Phase 3 test on a fresh machine: `bash infra/scripts/download-connect-plugins.sh` (see Phase 3 README / docs).

---

## 6. Phase 1 — Infrastructure

**Objective:** Three-broker KRaft cluster, Schema Registry, Kafka UI, Prometheus, Grafana; topics provisioned; **no ZooKeeper**; observability scrape paths alive.

### Startup (Phase 1 baseline)

```bash
bash infra/scripts/demo/start-stack-through-phase.sh 1
bash infra/scripts/demo/create-topics-and-registry.sh
```

Ensure JMX JAR is present (`download-jmx-agent.sh`) if Prometheus broker scrape should work.

### TC-P1-01 — Brokers and Registry reachable

| Step | Action |
|------|--------|
| 1 | `docker compose ps` — **kafka-1/2/3**, **schema-registry** healthy. |
| 2 | `docker exec -e KAFKA_OPTS='' kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:9092` |
| 3 | `curl -sf http://localhost:8081/subjects` |
| 4 | `curl -sf http://localhost:8080` (Kafka UI) |
| 5 | `curl -sf http://localhost:9090/-/healthy` |
| 6 | `curl -sf http://localhost:3000/api/health` |

**Expected:** All succeed (HTTP 200 / broker responds).

**Precautions:** None (no producer yet).

**Teardown:** None.

---

### TC-P1-02 — Topic layout and replication

**Startup:** Same as Phase 1 baseline.

**Steps:**

```bash
docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic raw-telemetry
docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic pit-predictions
```

**Expected:** Partition counts match **`create-topics.sh`** (e.g. raw **20**, pit **5**); **ReplicationFactor: 3** on cluster-default topics.

**Precautions:** Read-only; safe at any time.

---

### TC-P1-03 — Automated Phase 1 gate (partial)

**Startup:** Phase 1 baseline.

**Steps:** `bash infra/scripts/validate-cluster.sh` — passes **Kafka**, **topics**, **Registry**, **observability** sections.

**Expected:** Those sections **PASS** (later phases may fail if services not up — expected if you only started Phase 1).

**Precautions:** Full validate expects Phase 3–5 services for **all** checks — use this TC only to confirm **early sections**, or start full stack before **run-full-validation** (see §11).

---

## 7. Phase 2 — Telemetry producer

**Objective:** Avro **`TelemetryEvent`** to **`raw-telemetry`** at high configurable rate; simulator control API.

### Startup (through Phase 2)

```bash
bash infra/scripts/demo/start-stack-through-phase.sh 2
bash infra/scripts/demo/create-topics-and-registry.sh
```

### TC-P2-01 — Producer health and simulator control

**Steps:**

1. `curl -sf http://localhost:8090/actuator/health` — status **UP**.
2. `bash infra/scripts/demo/simulator-status.sh`
3. `bash infra/scripts/demo/simulator-start.sh`
4. Sleep **5s**; `bash infra/scripts/demo/simulator-status.sh` (running).
5. `docker exec -e KAFKA_OPTS='' kafka-1 kafka-get-offsets --bootstrap-server kafka-1:9092 --topic raw-telemetry`

**Expected:** Health UP; offsets increase over time for partitions.

**Precautions:** Before starting, set conservative **`SIMULATOR_EVENTS_PER_SECOND`** if needed (see §4).  
**Teardown:** `bash infra/scripts/demo/simulator-stop.sh` before Phase 3 tests if you want zero background load.

---

### TC-P2-02 — Schema registration for raw telemetry

**Steps:**

1. Start simulator briefly (`simulator-start.sh`).
2. `curl -sS http://localhost:8081/subjects | grep -i telemetry` — subject appears after first records.

**Expected:** **`raw-telemetry-value`** (or project naming) registered.

**Precautions:** Low EPS sufficient; **stop simulator** after confirming registration.

---

## 8. Phase 3 — CDC + stream enrichment

**Objective:** Debezium → **`circuit-metadata`** / **`driver-profiles`**; stream-processor → **`enriched-telemetry`** with **`EnrichedTelemetryEvent`**.

### Startup (through Phase 3)

```bash
bash infra/scripts/download-connect-plugins.sh   # once per machine
bash infra/scripts/demo/start-stack-through-phase.sh 3
bash infra/scripts/demo/create-topics-and-registry.sh
bash infra/scripts/demo/deploy-phase3-connectors.sh
```

**Postgres password vs Debezium:** `deploy-connectors.sh` verifies login over **TCP** to host **`postgres`** (`psql -h postgres`), matching how Kafka Connect reaches Postgres. A plain `psql` without `-h` can succeed via Unix socket while TCP auth still fails — do not rely on socket-only checks when debugging “password authentication failed”.

If TCP check fails or Connect returns **`password authentication failed`**, either set **`POSTGRES_PASSWORD`** in `.env` to the password your DB was **first initialized** with, or reset the Postgres volume (see §14) so init runs again with the current `.env`.

Wait until Connect REST shows connectors **RUNNING** (see validate script logic):  
`curl -sf http://localhost:8083/connectors/aerostream-circuits-connector/status`

### TC-P3-01 — Connectors and compacted topics

**Steps:**

1. `curl -sf http://localhost:8083/`  
2. `curl -sf http://localhost:8083/connectors/aerostream-circuits-connector/status`  
3. `docker exec -e KAFKA_OPTS='' kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 --topic circuit-metadata --from-beginning --max-messages 1 --timeout-ms 15000` (optional; **stop simulator first** to reduce noise).

**Expected:** Connector **RUNNING** with tasks **RUNNING**; compacted topic has at least one record (after Postgres seed + connector).

**Precautions:** Console consumer on shared laptop: **keep simulator stopped** or **low EPS** so the terminal remains usable.

**Teardown:** Stop simulator if you used console consumer.

---

### TC-P3-02 — Stream processor and enriched stream

**Steps:**

1. `curl -sf http://localhost:8091/actuator/health` — **UP**.
2. `bash infra/scripts/demo/simulator-start.sh` (moderate EPS).
3. After **30–60 s**, check offsets:  
   `docker exec -e KAFKA_OPTS='' kafka-1 kafka-get-offsets --bootstrap-server kafka-1:9092 --topic enriched-telemetry`

**Expected:** **`enriched-telemetry`** offsets advance; actuator UP.

**Precautions:** Do not run endless **`kafka-console-consumer`** on **`enriched-telemetry`** at full EPS — use **Kafka UI** with message limit or **stop simulator** after confirming offsets.

**Teardown:** `bash infra/scripts/demo/simulator-stop.sh`.

---

## 9. Phase 4 — ksqlDB analytics

**Objective:** Persistent queries; **`stream-aggregates`** JSON sink from hopping windows.

### Startup (through Phase 4)

```bash
bash infra/scripts/demo/start-stack-through-phase.sh 4
bash infra/scripts/demo/create-topics-and-registry.sh
bash infra/scripts/demo/deploy-phase3-connectors.sh
```

Ensure **`enriched-telemetry`** value schema exists in Registry (producer + stream-processor once).

```bash
bash infra/scripts/demo/simulator-start.sh   # brief run if needed, then stop
bash infra/scripts/demo/deploy-phase4-ksql.sh
```

### TC-P4-01 — ksqlDB alive and aggregate topic

**Steps:**

1. `curl -sf http://localhost:8088/info`
2. `docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic stream-aggregates`

**Expected:** ksqlDB responds; topic exists with **10 partitions** (per project config).

**Precautions:** If **`deploy-phase4-ksql.sh`** fails with schema errors, run producer + stream-processor until **`EnrichedTelemetryEvent`** is registered, then redeploy.

**Teardown:** `bash infra/scripts/demo/simulator-stop.sh`.

---

### TC-P4-02 — Aggregate messages flowing

**Steps:**

1. Moderate **`SIMULATOR_EVENTS_PER_SECOND`**.
2. `bash infra/scripts/demo/simulator-start.sh`
3. Wait **45–90 s** (windows are **30s** size / **10s** advance).
4. Use Kafka UI on **`stream-aggregates`** or **get-offsets** to confirm growth.

**Expected:** Non-zero throughput on **`stream-aggregates`**.

**Precautions:** Avoid max EPS while also opening heavy Grafana dashboards.

**Teardown:** **`simulator-stop.sh`**.

---

## 10. Phase 5 — ML consumer

**Objective:** Consume **`enriched-telemetry`**, emit **`PitPrediction`** to **`pit-predictions`**.

### Startup (through Phase 5)

```bash
bash infra/scripts/demo/start-stack-through-phase.sh 5
bash infra/scripts/demo/create-topics-and-registry.sh
bash infra/scripts/demo/deploy-phase3-connectors.sh
# After enriched schema exists:
bash infra/scripts/demo/deploy-phase4-ksql.sh
```

### TC-P5-01 — ML HTTP controls

**Steps:**

1. `curl -sf http://localhost:8099/health`
2. `curl -sf http://localhost:8099/ready` (may be **503** until partitions assigned — retry).

**Expected:** **200** on **health**; **ready** becomes **200** after consumer group assignment.

**Precautions:** None.

---

### TC-P5-02 — Pit predictions path

**Steps:**

1. Set moderate EPS; **`simulator-start.sh`**
2. Wait until **`/ready`** is **200** and **`pit-predictions`** offsets move:  
   `docker exec -e KAFKA_OPTS='' kafka-1 kafka-get-offsets --bootstrap-server kafka-1:9092 --topic pit-predictions`
3. Optional: Schema Registry lists **`pit-predictions-value`**.

**Expected:** Offsets increase; PitPrediction subject registered.

**Precautions:** Use **`CONSUMER_SLOWDOWN_MS`** on weak hardware; **stop simulator** before manual Avro decode sessions.

**Teardown:** **`simulator-stop.sh`**.

---

## 11. Full-stack demo (executive walkthrough)

**Goal:** One scripted path that demonstrates **all objectives** with controlled load.

### 11.1 Startup sequence (ordered)

```bash
# 0) One-time per environment
cp .env.example .env
bash infra/scripts/init-kafka-storage.sh
bash infra/scripts/download-jmx-agent.sh
bash infra/scripts/download-connect-plugins.sh

# 1) Conservative throughput for demo stability (optional but recommended)
export SIMULATOR_EVENTS_PER_SECOND=200
export CONSUMER_SLOWDOWN_MS=2

# 2) Stack through Phase 5
bash infra/scripts/demo/start-stack-through-phase.sh 5

# 3) Topics + Registry
bash infra/scripts/demo/create-topics-and-registry.sh

# 4) CDC connectors
bash infra/scripts/demo/deploy-phase3-connectors.sh

# 5) Seed pipelines so SR has EnrichedTelemetryEvent, then ksqlDB
bash infra/scripts/demo/simulator-start.sh
sleep 45
bash infra/scripts/demo/simulator-stop.sh
bash infra/scripts/demo/deploy-phase4-ksql.sh

# 6) Run steady-state demo traffic
bash infra/scripts/demo/simulator-start.sh
```

### 11.2 Observation checklist (5–10 minutes)

| Area | Where to look |
|------|----------------|
| Brokers / RF | Kafka UI — topics, replicas |
| Raw vs enriched | Topic message rates |
| CDC | **`circuit-metadata`**, **`driver-profiles`** |
| Aggregates | **`stream-aggregates`** |
| ML | **`pit-predictions`**, **`http://localhost:8099/metrics`** |
| Metrics | Prometheus **Targets**, Grafana |

### 11.3 Validation gate

When brokers, Connect, producer, stream-processor, and ML consumer are healthy and topics have data:

```bash
bash infra/scripts/demo/simulator-stop.sh   # optional: calmer validate
bash infra/scripts/demo/run-full-validation.sh
```

**Expected:** Script exits **0** — **all** checks PASS.

**Precautions:** If validation fails only on “topic has data”, restart simulator for **60–120 s**, stop, re-run validate — or keep **moderate** EPS during validate.

### 11.4 Demo teardown

```bash
bash infra/scripts/demo/simulator-stop.sh
# Optional: scale down for laptop
docker compose stop producer stream-processor ml-consumer ksqldb-server
# Or full tear-down:
# docker compose down
```

---

## 12. Optional soak / load characterization

**Purpose:** Stress **Kafka enhancements** (batching, ISR, retention) — **not** required for functional sign-off.

| Step | Action |
|------|--------|
| 1 | Raise **`SIMULATOR_EVENTS_PER_SECOND`** gradually (e.g. 2k → 10k) monitoring **`docker stats`** and broker health. |
| 2 | Watch **consumer lag** (Kafka UI consumer groups: stream-processor, **`aerostream-ml-consumer`**). |
| 3 | **Precaution:** Time-box soak (**5–15 min**); **`simulator-stop`** immediately if any broker restarts or host swaps. |

---

## 13. Post-run cleanup & stabilization

| Action | When |
|--------|------|
| **`simulator-stop.sh`** | After every session before switching test phases or closing laptop. |
| Lower **EPS** in `.env` | Default for day-to-day dev. |
| **`docker compose down`** | Free ports and RAM when done for the day (data volumes persist unless `-v`). |
| **Reset ksqlDB** | Only if needed: `bash infra/scripts/reset-ksql-queries.sh` (knows project semantics). |

---

## 14. Failure playbook (quick)

| Symptom | Check |
|---------|--------|
| **`deploy-connectors.sh` TCP check fails** or Connect **`password authentication failed`** | `.env` **`POSTGRES_PASSWORD`** must match the password stored **inside** Postgres (set only on first volume init). Fix: align `.env`, or **stop Postgres**, **`docker volume rm …postgres-data`**, **`docker compose up -d postgres`** (data loss). |
| Validate fails Phase 3 Connect | Connector REST **status**; Postgres up; plugins downloaded |
| No **enriched-telemetry** | Stream-processor logs; **`circuit-metadata`** / **`driver-profiles`** populated |
| ksqlDB deploy fails | **`EnrichedTelemetryEvent`** in SR — run producer + processor |
| ML **/ready** 503 forever | Brokers reachable from **`ml-consumer`** container; topic exists |
| Broker exit **137** | Increase Docker RAM; reduce EPS |

---

## Related documents

- Phase guides: **`docs/phase-1-infrastructure.md`** … **`docs/phase-5-ml-consumer.md`**
- Operational tracker: **`context.json`**
