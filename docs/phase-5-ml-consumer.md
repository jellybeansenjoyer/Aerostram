# Phase 5 — ML Inference Consumer (Pit Predictions)

**Status:** Complete  
**Total estimate:** ~8 hours  
**Issues covered:** ML-1 through ML-6  
**Stack:** Python 3.11 · FastAPI · Uvicorn · confluent-kafka-python · scikit-learn · Schema Registry (Avro) · Prometheus client · Docker Compose

---

## Table of Contents

1. [Who This Doc Is For](#who-this-doc-is-for)
2. [What Phase 5 Delivers](#what-phase-5-delivers)
3. [Concept Primer — Read This First If Phase 5 Is New](#concept-primer--read-this-first-if-phase-5-is-new)
4. [Why This Phase Exists](#why-this-phase-exists)
5. [How Phase 5 Fits After Phase 4](#how-phase-5-fits-after-phase-4)
6. [File Tree: New and Modified](#file-tree-new-and-modified)
7. [ML-1 — Output Contract: `PitPrediction` Avro Schema](#ml-1--output-contract-pitprediction-avro-schema)
8. [ML-2 — Training Script and the `.pkl` Model File](#ml-2--training-script-and-the-pkl-model-file)
9. [ML-3 — Turning Telemetry into Numbers (`predictor.py`)](#ml-3--turning-telemetry-into-numbers-predictorpy)
10. [ML-3 — Kafka Loop: Consume, Infer, Produce (`kafka_worker.py`)](#ml-3--kafka-loop-consume-infer-produce-kafka_workerpy)
11. [ML-4 — HTTP Control Plane (`main.py`)](#ml-4--http-control-plane-mainpy)
12. [ML-4 — Configuration (`config.py`)](#ml-4--configuration-configpy)
13. [ML-5 — Docker, Compose, Prometheus, Environment](#ml-5--docker-compose-prometheus-environment)
14. [ML-6 — Validation Script Updates](#ml-6--validation-script-updates)
15. [End-to-End Data Flow](#end-to-end-data-flow)
16. [Startup Sequence](#startup-sequence)
17. [Verification Commands](#verification-commands)
18. [Definition of Done](#definition-of-done)
19. [Key Design Decisions](#key-design-decisions)
20. [Troubleshooting](#troubleshooting)

---

## Who This Doc Is For

This guide assumes you **already understand Phases 1–4**: Kafka topics and partitions, Schema Registry and Avro, the telemetry producer, stream enrichment to **`enriched-telemetry`**, and optionally ksqlDB aggregates on **`stream-aggregates`**.

It does **not** assume you know **machine learning inference in Python**, **FastAPI**, or **how a model file is trained once and loaded at runtime**. Those ideas are introduced in the [Concept Primer](#concept-primer--read-this-first-if-phase-5-is-new) before we walk through files line by line.

---

## What Phase 5 Delivers

By the end of Phase 5 you have:

- A **`ml-consumer` Docker service** (container name **`aerostream-ml-consumer`**) that **reads** **`EnrichedTelemetryEvent`** records from **`enriched-telemetry`** (same Avro contract Phase 3 already publishes), runs a **small machine-learned model** to estimate **how urgent a pit stop is**, and **writes** **`PitPrediction`** records to **`pit-predictions`** (Avro, Schema Registry).
- A **new Avro schema** **`com.aerostream.avro.PitPrediction`** registered when the service first produces (BACKWARD-compatible additions elsewhere still follow global Registry rules).
- A **RandomForest** classifier trained **offline** during the Docker image build (`train_model.py` → **`pit_rf.pkl`**), loaded at startup — no training traffic on the live cluster.
- **Operational HTTP endpoints** on port **8099** (default): **`/health`** (process up), **`/ready`** (Kafka assigned partitions), **`/metrics`** (Prometheus text format).
- **Prometheus** scrape job for the ML consumer so you can graph throughput and errors next to brokers and Java services.
- **`validate-cluster.sh`** extended with a **Phase 5** section: health, readiness, **`pit-predictions`** partition count, and **records present** (needs the live pipeline feeding **`enriched-telemetry`**).

---

## Concept Primer — Read This First If Phase 5 Is New

### What is “ML inference” here versus “training”?

- **Training** means building the model from examples: showing the algorithm many input rows and labels until it learns patterns. In AeroStream this happens **once inside `docker build`** when **`train_model.py`** runs. It writes **`models/pit_rf.pkl`** (a serialized sklearn model).
- **Inference** (or **scoring**) means: **for each new Kafka message**, turn its fields into a **feature vector**, ask the loaded model for an output, and emit the result. That runs **continuously** in the consumer loop — no gradient descent, no epochs, just fast math on each event.

So: **train offline, predict online**. That keeps the hot path simple and predictable.

### What is a RandomForest classifier (intuition only)?

A **RandomForest** is an ensemble of many **decision trees**. Each tree asks simple yes/no questions on the input features (“is wear above X?”). Trees **vote** on a class (here: “pit soon” vs “not”). **`predict_proba`** returns a **probability** between 0 and 1 for the positive class — we treat that as **`pit_probability`**.

We use it because it is **robust**, **interpretable enough for a demo**, and **available in scikit-learn** without extra infrastructure (no separate model server required).

### What are “features” and why normalize them?

A **feature** is one number the model sees — e.g. average tire wear. **Normalization** puts different sensors on a comparable scale (wear 0–100%, fuel in kg, speed in kph). Our code compresses them into roughly **[0, 1]** so they match how **`train_model.py`** generated synthetic training data (also in **[0, 1]** per dimension).

If training and inference use **different** formulas, predictions are meaningless — so **`predictor.py`** mirrors the training layout **exactly** (six dimensions in fixed order).

### Why consume `enriched-telemetry` and not only `stream-aggregates`?

**`enriched-telemetry`** is **per-sample**, high cadence — good for “right now, should this car pit?” **`stream-aggregates`** (Phase 4) is **windowed KPIs** — useful for dashboards; you could add a second consumer later that reads aggregates for slower tactical summaries. Phase 5 chooses the **richest per-event** stream so the model sees tire corners and fuel directly.

### Consumer group (quick recap)

The ML consumer uses **`group.id=aerostream-ml-consumer`**. All instances of this service share offsets for **`enriched-telemetry`** partitions. One instance is enough for local dev; scaling out partitions work across brokers like any other consumer group.

### Why Python instead of another Java service?

**scikit-learn** and **joblib** are Python-native. Shipping a **`.pkl`** and **small scripts** is simpler than embedding sklearn in the JVM. Kafka doesn’t care — it’s just another consumer/producer.

### FastAPI and Uvicorn (why HTTP on an ML worker?)

**Kubernetes/Docker** ecosystems expect **liveness** and **readiness** probes over HTTP. FastAPI exposes **`/health`** and **`/ready`** without building a full Spring Boot app. **Uvicorn** is the ASGI server that runs the FastAPI app.

### `/health` vs `/ready`

- **`/health`** — “Is the Python process and web server up?” Returns quickly even if Kafka isn’t ready yet.
- **`/ready`** — “Has the consumer **joined the group** and **received partition assignment**?” Until then it returns **503**, so orchestrators don’t route traffic if the consumer cannot actually process.

### Prometheus `/metrics`

We expose **Counters**: messages consumed, predictions emitted, errors. Prometheus scrapes **text format** on **`/metrics`** — same idea as Spring Boot **`/actuator/prometheus`**, lighter setup.

### Dependencies you might not have seen: `requests`, `fastavro`

Confluent’s **Schema Registry client** uses **`requests`** for HTTP calls to register and fetch schemas. **Avro** serialization in this stack uses **`fastavro`** under the hood. They are listed explicitly in **`requirements.txt`** so a minimal image doesn’t miss transitive pieces.

---

## Why This Phase Exists

**Problem:** Operations and strategy tools often want **forward-looking signals** (“this car likely needs a pit soon”), not only raw numbers. Phase 3–4 give you streams of facts; Phase 5 adds a **decision-support score** in Kafka so any subscriber (dashboard, alerting, another service) can react without re-implementing physics.

**Approach:** A dedicated **ML consumer** keeps **training artifacts** and **Python ML libraries** out of the Java producer and stream-processor, preserves **Schema Registry** as the contract hub, and writes a **small, stable output schema** (`PitPrediction`) to **`pit-predictions`**.

---

## How Phase 5 Fits After Phase 4

```
Producer (Phase 2)      →  raw-telemetry
stream-processor (3)    →  enriched-telemetry     (Avro)
ksqlDB (4)              →  stream-aggregates      (JSON windows)
ml-consumer (5)         ←  enriched-telemetry
ml-consumer (5)         →  pit-predictions        (Avro)
```

Phase 5 **does not replace** Phase 4; it **consumes** enriched events and **adds** a prediction topic.

---

## File Tree: New and Modified

### New (`ml-consumer/`)

```
ml-consumer/
├── Dockerfile                 ← Python image; trains model at build; runs Uvicorn
├── requirements.txt           ← confluent-kafka, requests, fastavro, sklearn, FastAPI, …
├── .dockerignore
├── train_model.py             ← offline RandomForest training → models/pit_rf.pkl
├── schemas/
│   └── PitPrediction.avsc     ← output Avro schema
├── models/
│   └── .gitkeep               ← pickle produced inside Docker build (not committed)
└── app/
    ├── __init__.py
    ├── config.py              ← env-driven Settings
    ├── predictor.py           ← feature extraction + sklearn predict_proba
    ├── kafka_worker.py        ← Consumer loop + Producer + Avro serde
    └── main.py                ← FastAPI app, lifespan, metrics, health/ready
```

### Modified elsewhere

| Area | Change |
|------|--------|
| `docker-compose.yml` | `ml-consumer` service, port **8099**, depends on brokers + Schema Registry |
| `infra/prometheus/prometheus.yml` | Scrape job **`aerostream-ml-consumer:8099/metrics`** |
| `infra/scripts/validate-cluster.sh` | Phase 5 checks |
| `.env.example` | `ML_CONSUMER_PORT`, optional `PIT_THRESHOLD`, `CONSUMER_SLOWDOWN_MS` |
| `README.md` | Phase 5 Done, endpoints, quick-start order |

---

## ML-1 — Output Contract: `PitPrediction` Avro Schema

**File:** `ml-consumer/schemas/PitPrediction.avsc`

| Field | Type | Meaning |
|-------|------|--------|
| `car_id` | string | Same identifier as telemetry (`CAR_01`, …) — also used as Kafka **message key** for ordering per car. |
| `timestamp_ms` | long | Event time carried through from enriched telemetry for correlation. |
| `pit_probability` | double | Model output in **[0, 1]** — estimated chance the “pit soon” class applies. |
| `recommend_pit` | boolean | **`true`** if **`pit_probability >= PIT_THRESHOLD`** (default **0.65**). Tunable without retraining. |
| `model_version` | string | e.g. **`rf-1.0.0`** — lets downstream distinguish schema/model generations. |

**Why Avro again?** Same governance as Phase 2–3: evolution under BACKWARD rules, readable in Kafka UI with Registry integration.

---

## ML-2 — Training Script and the `.pkl` Model File

**File:** `train_model.py`

The script is **not** learning from your live race replay in Docker — it generates **synthetic** random rows in **[0, 1]^6** and labels **`y`** with a simple rule (high wear, or medium wear plus low fuel). That teaches the forest a **consistent** mapping so **`predictor.py`** can feed real normalized telemetry into the same six dimensions.

Line-by-line:

| Lines | What happens |
|-------|----------------|
| 11–12 | **`FEATURE_DIM = 6`** must match **`_row_from_event`** output width. **`MODEL_PATH`** points under **`models/pit_rf.pkl`**. |
| 15–18 | **`10_000`** random training rows — enough for a small forest to stabilize without slow builds. |
| 19–22 | **Labels**: positive class when synthetic wear column **`> 0.72`** OR (**wear > 0.52** AND **fuel < 0.28**) — mimics “needs pit” scenarios. |
| 23–29 | **`RandomForestClassifier`** with bounded depth and **`random_state=42`** for **reproducible** builds. |
| 30 | **`fit`** learns from **`(x, y)`**. |
| 31–32 | Ensure **`models/`** exists; **`joblib.dump`** writes the pickle consumed at runtime. |

**Why synthetic training?** Shipping a reproducible pipeline without checking gigabytes of telemetry into git. In production you’d replace this with historical data — the **plumbing** stays the same.

---

## ML-3 — Turning Telemetry into Numbers (`predictor.py`)

**File:** `app/predictor.py`

**`_row_from_event`** builds **one row** of six floats aligned with training:

| Index | Source idea | Formula intuition |
|-------|-------------|-------------------|
| 0 | Wear | Average of four tire wear sensors ÷ **100** (wear is 0–100%). |
| 1 | Temperature | Average tire temps ÷ **150** (rough scale). |
| 2 | Fuel | **`fuel_load_kg` / 120** — upper bound guess for normalization. |
| 3 | Speed | **`speed_kph` / 380**. |
| 4 | ERS | **`ers_deploy_pct` / 100**. |
| 5 | Lap progress | **`min(lap, 100) / 100`** — caps odd outliers. |

**`np.clip(..., 0, 1)`** prevents extreme values if telemetry spikes.

**`Predictor`** loads **`joblib.load`** once. **`pit_probability`** calls **`predict_proba`** and takes **`[0, 1]`** — probability of class **1** (“positive” / pit-soon in training).

---

## ML-3 — Kafka Loop: Consume, Infer, Produce (`kafka_worker.py`)

**Setup (lines 36–61)**  
Creates **SchemaRegistryClient**, **`AvroDeserializer`** (reads **wire-format** messages whose schema ID is embedded — usual Confluent pattern), loads **`PitPrediction.avsc`** text for **`AvroSerializer`**. Separate **`Consumer`** and **`Producer`** configs; **`auto.offset.reset=earliest`** helps catch up in dev.

**Readiness (lines 63–76)**  
Before processing messages, once **`consumer.assignment()`** is non-empty, **`on_ready()`** fires so **`/ready`** can flip to **200**. This covers the case where the topic exists but **no message arrived yet** — assignment alone means Kafka considers the consumer ready to read.

**Poll loop (lines 71–128)**  
- **`poll(1.0)`** waits up to one second per iteration — normal Kafka pattern.  
- **`PARTITION_EOF`** is ignored (harmless notification at end of partition when reading).  
- Deserialize with **`SerializationContext(topic, MessageField.VALUE)`**.  
- **`predictor.pit_probability(event)`** → threshold → build **`out`** dict matching **`PitPrediction`** field names.  
- **`producer.produce`** with **`on_delivery`** callback; **`producer.poll(0)`** lets delivery callbacks run; **`flush`** on shutdown.

**`CONSUMER_SLOWDOWN_MS`** optional sleep — throttle local laptop overload.

---

## ML-4 — HTTP Control Plane (`main.py`)

| Lines | Role |
|-------|------|
| 19–30 | Prometheus **Counters** incremented from the worker thread (thread-safe enough for simple counters here). |
| 33–63 | **`lifespan`**: load **`Predictor`**, start **`run_consumer_loop`** in a **daemon thread**, store **`kafka_ready`** on **`app.state`**, on shutdown signal **`stop_event`** and **join** the worker. |
| 73–76 | **`/health`** — always **200** if Uvicorn runs. |
| 78–86 | **`/ready`** — **503** until **`kafka_ready`** is set (partition assignment path). |
| 89–92 | **`/metrics`** — Prometheus exposition format. |

---

## ML-4 — Configuration (`config.py`)

**`Settings`** reads environment variables with sensible defaults for Docker Compose service names (`kafka-1:9092`, …). Key tunables:

- **`PIT_THRESHOLD`** — business knob for **`recommend_pit`** without changing code.  
- **`MODEL_PATH`** — default **`/app/models/pit_rf.pkl`** inside the image.  
- **`INPUT_TOPIC` / `OUTPUT_TOPIC`** — override if you fork topics.

---

## ML-5 — Docker, Compose, Prometheus, Environment

**Dockerfile**  
Installs build deps + **curl** for Compose **healthcheck**. Order: **`pip install`** → **`train_model.py`** → copy **`app/`**. **`MODEL_PATH`** env points at the generated pickle.

**`docker-compose.yml`**  
Service **`ml-consumer`** publishes **`8099`**, depends on **three healthy brokers** and **healthy Schema Registry** (same pattern as other clients). Stream-processor is **not** a hard dependency in Compose — you must **start producer + stream-processor** so **`enriched-telemetry`** actually has data.

**`infra/prometheus/prometheus.yml`**  
Job **`aerostream-ml-consumer`** scrapes **`/metrics`** every 15s like other services.

**`.env.example`**  
Documents **`ML_CONSUMER_PORT`**, optional **`PIT_THRESHOLD`**, **`CONSUMER_SLOWDOWN_MS`**.

---

## ML-6 — Validation Script Updates

**`infra/scripts/validate-cluster.sh`** banner now says **Phases 1–5**. Phase 5 block:

1. **`/health`** — process reachable.  
2. **`/ready`** — consumer assigned partitions.  
3. **`pit-predictions`** describes **PartitionCount: 5**.  
4. **`topic_has_records pit-predictions`** — confirms the ML consumer **produced** at least one message (needs upstream telemetry flowing).

Items **3–4** fail if topics weren’t created or **no producer + stream-processor** feed **`enriched-telemetry`**.

---

## End-to-End Data Flow

1. **Producer** emits **`TelemetryEvent`** → **`raw-telemetry`**.  
2. **Stream-processor** emits **`EnrichedTelemetryEvent`** → **`enriched-telemetry`** (Schema Registry subject **`enriched-telemetry-value`**).  
3. **ML consumer** polls **`enriched-telemetry`**, deserializes Avro to a **dict**, computes **`pit_probability`**, serializes **`PitPrediction`**, produces to **`pit-predictions`** keyed by **`car_id`**.  
4. **Observers** (Kafka UI, Grafana via Prometheus lag metrics, custom apps) subscribe to **`pit-predictions`**.

---

## Startup Sequence

1. **`docker compose up -d`** — brings up **`ml-consumer`** after brokers + Registry are healthy.  
2. **`bash infra/scripts/create-topics.sh`** — ensures **`pit-predictions`** exists among others.  
3. **`bash infra/scripts/configure-schema-registry.sh`** — BACKWARD config (Phase 1).  
4. Start **producer** and **stream-processor** (and Phase 4 **ksqlDB** deploy if you need aggregates).  
5. **`bash infra/scripts/validate-cluster.sh`** — full gate including Phase 5.

---

## Verification Commands

```bash
# ML HTTP
curl -sS http://localhost:8099/health
curl -sS http://localhost:8099/ready

# Pit predictions topic
docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
  --describe --topic pit-predictions

# Schema registered after first produce
curl -sS http://localhost:8081/subjects | jq '.[] | select(test("pit"))'
```

---

## Definition of Done

- **`docker compose ps`** shows **`aerostream-ml-consumer`** healthy (or at least **`/health`** OK while Kafka catches up).  
- **`/ready`** returns **200** after partition assignment.  
- With **producer + stream-processor** running, **`pit-predictions`** gains offsets (**`topic_has_records`** passes).  
- **`bash infra/scripts/validate-cluster.sh`** exits **0**.  
- Prometheus shows target **`aerostream-ml-consumer`** UP if Prometheus is running.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Train in **Docker build**, not at container start | Faster startup, deterministic image, no CPU spike on every **`docker compose up`**. |
| **Six hand-crafted features** | Explainable, matches synthetic training; easy to extend later. |
| **`recommend_pit` separate from probability** | Operators tune threshold without retraining. |
| **Plain `Consumer`/`Producer` + serde helpers** | Explicit control; avoids hiding partition semantics from learners. |
| **Thread for Kafka, async for FastAPI** | Simple **`threading`** bridge without rewriting the stack on **`asyncio`**. |

---

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| **`/ready` always 503** | Consumer cannot join (broker down, ACL, typo in **`KAFKA_BOOTSTRAP_SERVERS`**). |
| **Deserialize errors** | **`enriched-telemetry`** subject missing — run producer + stream-processor once so Registry has **`EnrichedTelemetryEvent`**. |
| **`pit-predictions` empty** | No input traffic; ML never gets messages to publish after inference. |
| **Validation fails “pit-predictions has data”** | Same — start simulator/producer and stream-processor; wait for lag to drain. |
| **Import errors for `requests` / `fastavro`** | Old image — rebuild **`ml-consumer`** from current **`requirements.txt`**. |

---

## Related References

- **`context.json`** — structured issue breakdown (**ML-1** … **ML-6**) and completion dates.  
- Phase 3 **`EnrichedTelemetryEvent`** — field definitions **`predictor.py`** relies on.  
- **`infra/scripts/create-topics.sh`** — **`pit-predictions`** partitions (**5**) and retention.
