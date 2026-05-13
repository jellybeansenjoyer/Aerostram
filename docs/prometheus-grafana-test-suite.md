# AeroStream — Prometheus & Grafana test suite (Phases 1–5)

Step-by-step checks for **metrics collection** (Prometheus) and **visualization** (Grafana), aligned with project objectives: broker/JMX observability (Phase 1), producer Micrometer + dashboard (Phase 2), Connect + stream-processor scrapes (Phase 3), streaming analytics (Phase 4 — **no dedicated Prometheus scrape** in-repo), ML consumer metrics (Phase 5).

**Related:** [`demo-system-test-suite.md`](./demo-system-test-suite.md) (full pipeline), [`infra/prometheus/prometheus.yml`](../infra/prometheus/prometheus.yml), [`infra/grafana/`](../infra/grafana/).

---

## How to read a test case

| Field | Meaning |
|-------|---------|
| **TC-PG-P{n}-{nn}** | Test case ID (Prometheus/Grafana). |
| **Requirement** | Which original capability this validates. |
| **Startup** | Stack slice that must be running first. |
| **Steps** | Ordered actions. |
| **Expected** | Pass criteria. |
| **Precautions** | Throughput or ordering notes. |

Default URLs (override with `.env`): Prometheus `http://localhost:9090`, Grafana `http://localhost:3000` (often **admin/admin**).

---

## Requirement traceability (observability)

| Requirement | Prometheus | Grafana |
|-------------|------------|---------|
| Phase 1 — JMX on 3 brokers, 15s scrape, 15d retention | Jobs **`kafka-broker-{1,2,3}-jmx`** → `kafka-*:7071` (staggered intervals) | Datasource → Prometheus; optional Explore |
| Phase 1 — Schema Registry metrics | `schema-registry` job → **`:7072/metrics`** (JMX javaagent; REST API stays **:8081**) | Same datasource |
| Phase 1 — Grafana provisioning | — | Auto-loaded **Prometheus** datasource; **AeroStream** dashboard folder |
| Phase 2 — Producer Micrometer | `aerostream-producer` → `:8090/actuator/prometheus` | **AeroStream — Producer** dashboard (`producer-dashboard.json`) |
| Phase 3 — Kafka Connect JVM/worker | `kafka-connect` → `:7073/metrics` (JMX javaagent; REST `:8083` has no `/metrics`) | Explore / Explore metrics |
| Phase 3 — Stream processor Actuator | `aerostream-stream-processor` → `:8091/actuator/prometheus` | Explore |
| Phase 4 — ksqlDB analytics | **No scrape job** in `prometheus.yml` | Use **Explore** with broker/topic metrics if needed; health via `validate-cluster` / `:8088/info` |
| Phase 5 — ML consumer counters | `aerostream-ml-consumer` → `:8099/metrics` | Explore |

---

## Global prerequisites (observability)

1. **`cp .env.example .env`**, **`KAFKA_CLUSTER_ID`**: `bash infra/scripts/init-kafka-storage.sh`.
2. **JMX agent:** `bash infra/scripts/download-jmx-agent.sh` before brokers start (Phase 1 broker targets **UP**).
3. **Ports:** `9090` (Prometheus), `3000` (Grafana), plus phase-specific ports (8090, 8091, 8083, 8099).
4. **Grafana datasource UID:** `infra/grafana/provisioning/datasources/prometheus.yml` sets **`uid: prometheus`** so **`producer-dashboard.json`** panels resolve without manual edits.

---

## Phase 1 — Platform metrics & Grafana baseline

**Objective:** Prometheus scrapes **Kafka JMX** (7071), **Schema Registry**; Grafana is healthy and can query Prometheus.

### TC-PG-P1-01 — Prometheus server health

| Field | Content |
|-------|---------|
| **Requirement** | Operability: Prometheus reachable (`validate-cluster` pattern). |
| **Startup** | `bash infra/scripts/demo/start-stack-through-phase.sh 1` |
| **Steps** | 1. `curl -sf http://localhost:9090/-/healthy` → expect non-empty body / HTTP 200.<br>2. Open `http://localhost:9090` → UI loads.<br>3. **Status → Configuration** → confirm `scrape_interval: 15s`, retention consistent with compose (**15d** TSDB). |
| **Expected** | Healthy endpoint OK; UI reachable; global scrape interval **15s**. |
| **Precautions** | None. |

---

### TC-PG-P1-02 — All Phase 1 scrape targets UP

| Field | Content |
|-------|---------|
| **Requirement** | Phase 1 INFRA-5: brokers JMX + Schema Registry on scrape path (`prometheus.yml`). |
| **Startup** | Phase 1 baseline; JMX JAR downloaded; brokers healthy. |
| **Steps** | 1. Open **Status → Targets** (`http://localhost:9090/targets`).<br>2. Find jobs **`kafka-broker-1-jmx`**, **`kafka-broker-2-jmx`**, **`kafka-broker-3-jmx`**, and **`schema-registry`**. <br>3. For each **kafka-broker-*-jmx** job: one target (`kafka-1:7071`, etc.), **State = UP**.<br>4. For **`schema-registry`**: target **`schema-registry:7072`**, path `/metrics`, **UP** (not `:8081` — that is REST only).<br>5. Optional CLI: `curl -sf http://localhost:9090/api/v1/targets` → grep `"health":"up"` for those instances. |
| **Expected** | All listed targets **UP** (allow ~30–60s after broker healthy). If a broker is **DOWN**, check JMX agent mount and `KAFKA_OPTS` on that broker. |
| **Precautions** | Targets flip to DOWN during broker restart — recheck after `docker compose ps` shows healthy. |

---

### TC-PG-P1-03 — Broker JMX metrics in Prometheus (JMX rules)

| Field | Content |
|-------|---------|
| **Requirement** | JMX exporter rules in `infra/prometheus/jmx-config/kafka-jmx.yml` (BrokerTopicMetrics, ReplicaManager, KafkaController, RequestMetrics, etc.). |
| **Startup** | Phase 1; TC-PG-P1-02 passes. |
| **Steps** | 1. **Graph** → run at least one query that should exist per rule file, e.g.: `kafka_network_requests_total`<br>2. `kafka_server_broker_topic_*` or `kafka_server_replica_manager_*`<br>3. `kafka_controller_*`<br>4. Confirm **non-empty** results or **matrix** with labels (`topic`, `request`, …). |
| **Expected** | Metrics appear (values may be 0 until traffic — label series still prove scrape). |
| **Precautions** | Very low traffic → some counters zero; still valid if series exist. |

---

### TC-PG-P1-04 — Grafana health & Prometheus datasource

| Field | Content |
|-------|---------|
| **Requirement** | Grafana provisioning: datasource points at **`http://prometheus:9090`**, proxy mode (`infra/grafana/provisioning/datasources/prometheus.yml`). |
| **Startup** | Phase 1 baseline. |
| **Steps** | 1. `curl -sf http://localhost:3000/api/health` → response contains **`"database":"ok"`** (or grep `ok`).<br>2. Login Grafana → **Connections → Data sources → Prometheus**.<br>3. **Save & test** → **“Data source is working”**.<br>4. Confirm **URL** `http://prometheus:9090`, **Default** checked if intended. |
| **Expected** | Health OK; test succeeds from inside Grafana container network. |
| **Precautions** | If test fails, verify `prometheus` service name on `aerostream-network` and Prometheus container running. |

---

### TC-PG-P1-05 — Grafana Explore (sanity PromQL)

| Field | Content |
|-------|---------|
| **Requirement** | End-to-end: Grafana → Prometheus queries return data. |
| **Startup** | TC-PG-P1-04 passes. |
| **Steps** | 1. **Explore** → datasource **Prometheus**.<br>2. Query `up{job=~"kafka-broker-[123]-jmx"}` → **three** series with value **1**.<br>3. Query `up{job="schema-registry"}` → value **1**. |
| **Expected** | Both queries succeed; `up` reflects target health. |
| **Precautions** | None. |

---

### TC-PG-P1-06 — Dashboard folder provisioning

| Field | Content |
|-------|---------|
| **Requirement** | Dashboard provider **AeroStream** folder (`infra/grafana/provisioning/dashboards/provider.yml`). |
| **Startup** | Phase 1; Phase 2 dashboard file present on volume (may load after Phase 2 image mount — usually dashboards dir mounted). |
| **Steps** | 1. **Dashboards** → browse folder **AeroStream**.<br>2. After Phase 2 producer image/dashboard present: open **AeroStream — Producer** (UID `aerostream-producer`) — if missing, confirm `infra/grafana/dashboards/producer-dashboard.json` is mounted at `/var/lib/grafana/dashboards`. |
| **Expected** | **AeroStream** folder exists; Producer dashboard appears once provisioned JSON is visible to Grafana. |
| **Precautions** | Dashboard JSON ships in repo — first-time Grafana may take **≤30s** (`updateIntervalSeconds`) to load. |

---

## Phase 2 — Producer Micrometer & Producer Grafana dashboard

**Objective:** Job **`aerostream-producer`** scrapes **`/actuator/prometheus`**; Micrometer names match **`producer-dashboard.json`**; panels show live data under load.

### TC-PG-P2-01 — Producer scrape target UP

| Field | Content |
|-------|---------|
| **Requirement** | Phase 2 PROD-6: Prometheus scrape job `aerostream-producer` (`prometheus.yml`). |
| **Startup** | `bash infra/scripts/demo/start-stack-through-phase.sh 2` + topics/registry scripts as in demo doc. |
| **Steps** | 1. **Targets:** job **`aerostream-producer`**, instance **`aerostream-producer:8090`**, path **`/actuator/prometheus`**, **UP**.<br>2. `curl -sf http://localhost:8090/actuator/prometheus` → Prometheus **text** with `# HELP` lines. |
| **Expected** | Target **UP**; endpoint returns exposition format. |
| **Precautions** | Producer depends on brokers + Schema Registry healthy. |

---

### TC-PG-P2-02 — Micrometer metric names in Prometheus

| Field | Content |
|-------|---------|
| **Requirement** | PROD-6: `telemetry.events.published`, DLQ, active cars → Prometheus names (`telemetry_events_published_total`, …). |
| **Startup** | Phase 2; simulator **started** (`simulator-start.sh` or POST `/api/simulator/start`). |
| **Steps** | In **Graph** / **Explore**, verify series exist:<br>• `telemetry_events_published_total{service="producer"}` (counter increases)<br>• `telemetry_events_dlq_total{service="producer"}`<br>• `telemetry_active_cars`<br>• Kafka client metrics used by dashboard: `kafka_producer_record_send_total`, `kafka_producer_request_latency_avg` |
| **Expected** | Counters/gauges present; **published** rate **> 0** while simulator runs. |
| **Precautions** | High EPS → narrow time range in Grafana if UI slows (see demo doc precautions). |

---

### TC-PG-P2-03 — Grafana Producer dashboard — every panel

| Field | Content |
|-------|---------|
| **Requirement** | Grafana **AeroStream — Producer** — all panels bound to Prometheus expressions in `producer-dashboard.json`. |
| **Startup** | Phase 2; simulator **running**; wait **≥2 scrape intervals (30s)** after start. |
| **Steps** | Open dashboard **AeroStream — Producer** (`uid` **aerostream-producer**). Confirm each panel shows **No data** only when metrics legitimately absent (e.g. simulator stopped): |

| Panel # | Title | PromQL (summary) | Expected with simulator ON |
|--------|-------|-------------------|----------------------------|
| 1 | Events Published / sec | `rate(telemetry_events_published_total{service="producer"}[1m])` | Positive rate |
| 2 | DLQ Rate / sec | `rate(telemetry_events_dlq_total{service="producer"}[1m])` | ≥ 0 (often near 0) |
| 3 | Active Cars | `telemetry_active_cars` | Matches simulator (e.g. **20**) |
| 4 | Total Events Published | `telemetry_events_published_total{service="producer"}` | Increasing total |
| 5 | Total DLQ Events | `telemetry_events_dlq_total{service="producer"}` | Stable unless poisoning |
| 6 | Kafka Producer Record Send Rate | `rate(kafka_producer_record_send_total[1m])` | Positive rate |
| 7 | Producer Request Latency (avg) | `kafka_producer_request_latency_avg` | Numeric ms series |

| **Expected** | All seven panels render **without datasource errors**; panels 1–4–6–7 show activity under load. |
| **Precautions** | If **service="producer"** label missing on Micrometer metrics, sync app config with dashboard label convention or adjust PromQL. |

---

### TC-PG-P2-04 — DLQ path observable (optional negative test)

| Field | Content |
|-------|---------|
| **Requirement** | DLQ routing observable via **`telemetry_events_dlq_total`** (non-production poison pill). |
| **Startup** | Phase 2; non-production profile allowing test controller if applicable. |
| **Steps** | 1. Trigger small DLQ load per Phase 2 doc (e.g. poison pill endpoint if enabled).<br>2. Panel **DLQ Rate / sec** and **Total DLQ Events** spike or increase.<br>3. Confirm **Events Published / sec** still coherent. |
| **Expected** | DLQ counters reflect failures; Grafana shows non-zero DLQ path. |
| **Precautions** | Do not run poison tests on production profile. |

---

## Phase 3 — Kafka Connect & stream-processor scrapes

**Objective:** Jobs **`kafka-connect`** and **`aerostream-stream-processor`** are **UP**; metrics endpoints reachable; optional JVM/stream metrics in Explore.

### TC-PG-P3-01 — Kafka Connect target UP

| Field | Content |
|-------|---------|
| **Requirement** | Phase 3: Prometheus scrape Connect **`:7073/metrics`** (JMX javaagent). REST **`:8083`** is the Connect API only — **`/metrics` there is 404**. |
| **Startup** | Through Phase 3 (`start-stack-through-phase.sh 3`), Connect healthy, plugins loaded. |
| **Steps** | 1. **Targets:** job **`kafka-connect`**, endpoint **`kafka-connect:7073`**, **UP**.<br>2. `curl -sf http://localhost:${KAFKA_CONNECT_METRICS_PORT:-7073}/metrics` (host maps **7073** → exporter) → Prometheus text **# HELP**.<br>3. Explore: `up{job="kafka-connect"}` → **one** series with value **1** (not `:8083` — that path 404). |
| **Expected** | Target UP; exposition OK. |
| **Precautions** | Connect starts after brokers; wait for healthy container. |

---

### TC-PG-P3-02 — Stream processor target UP & Actuator prometheus

| Field | Content |
|-------|---------|
| **Requirement** | Phase 3: scrape **`aerostream-stream-processor:8091/actuator/prometheus`**. |
| **Startup** | Phase 3; stream-processor running; Phase 2–3 data path optional for JVM metrics. |
| **Steps** | 1. **Targets:** **`aerostream-stream-processor`**, **UP**.<br>2. `curl -sf http://localhost:8091/actuator/prometheus` → text exposition.<br>3. Explore: `up{job="aerostream-stream-processor"}` → **1**.<br>4. Optional: `jvm_memory_used_bytes{job="aerostream-stream-processor"}` or `kafka_streams_*` if exposed. |
| **Expected** | Scrape UP; JVM/Kafka Streams related series discoverable. |
| **Precautions** | First scrape after startup may take one interval (**15s**). |

---

### TC-PG-P3-03 — End-to-end label sanity (cluster label)

| Field | Content |
|-------|---------|
| **Requirement** | `cluster: aerostream-local` on Connect / stream-processor jobs (`prometheus.yml`). |
| **Steps** | Query e.g. `kafka_connect_*` or JVM metrics with label filter `cluster="aerostream-local"` where applied; or inspect **Targets** labels in UI. |
| **Expected** | Labels match config for jobs that define `cluster`. |
| **Precautions** | Broker job uses same cluster label — useful for multi-env dashboards later. |

---

## Phase 4 — ksqlDB & analytics (Prometheus scope)

**Objective:** Clarify what **is** and **is not** scraped; use Grafana Explore on **Kafka** metrics if needed for operational visibility.

### TC-PG-P4-01 — ksqlDB not a Prometheus target (documented gap)

| Field | Content |
|-------|---------|
| **Requirement** | Phase 4 delivers **ksqlDB** streaming SQL; **no `ksqldb` scrape_config** in `prometheus.yml`. |
| **Startup** | Phase 4 running (`ksqldb-server` healthy, queries deployed). |
| **Steps** | 1. **Targets** list: confirm **no** dedicated ksqlDB scrape job.<br>2. Cluster validation: `curl -sf http://localhost:8088/info` (per `validate-cluster`).<br>3. Optional: Explore broker metrics related to internal topics (e.g. `_confluent-ksql-*` topic traffic via `kafka_server_broker_topic_*`) — **informational**, not a committed dashboard in-repo. |
| **Expected** | **Health** via REST `/info`; Prometheus **does not** expose first-class ksqlDB JVM scrape in current repo — note for roadmap if parity with other services is required. |
| **Precautions** | Do not fail Phase 4 pipeline tests solely because ksqlDB is absent from Prometheus targets. |

---

### TC-PG-P4-02 — Stream-aggregates traffic indirectly (optional)

| Field | Content |
|-------|---------|
| **Requirement** | Analytics sink **`stream-aggregates`** exists; broker metrics may show topic traffic. |
| **Startup** | Phase 4 persistent query writing to **`stream-aggregates`**; producer + processor feeding **enriched-telemetry**. |
| **Steps** | Explore: query broker topic metrics matching **`stream-aggregates`** label if exposed via `kafka_server_broker_topic_*` / topic tag per JMX rules. |
| **Expected** | Non-zero activity correlates with sustained ingestion (qualitative). |
| **Precautions** | JMX topic naming must match Kafka internal naming — use **Metrics** tab in Kafka UI as cross-check. |

---

## Phase 5 — ML consumer Prometheus metrics

**Objective:** Job **`aerostream-ml-consumer`** scrapes **`/metrics`**; counters **`ml_consumer_*`** increase during inference.

### TC-PG-P5-01 — ML consumer target UP

| Field | Content |
|-------|---------|
| **Requirement** | Phase 5 ML-5: scrape **`aerostream-ml-consumer:8099/metrics`**. |
| **Startup** | Through Phase 5; ML container healthy. |
| **Steps** | 1. **Targets:** **`aerostream-ml-consumer`**, **UP**.<br>2. `curl -sf http://localhost:8099/metrics` → text with **`ml_consumer_messages_consumed_total`**, **`ml_consumer_predictions_emitted_total`**, **`ml_consumer_errors_total`**. |
| **Expected** | Target UP; three counters present in exposition. |
| **Precautions** | `/ready` may be 503 until partition assignment — metrics endpoint still up; scrape state should still be UP if process listens. |

---

### TC-PG-P5-02 — Counter monotonicity under load

| Field | Content |
|-------|---------|
| **Requirement** | ML inference path increments consumed/emitted; errors observable. |
| **Startup** | Phase 5; **`enriched-telemetry`** has traffic; ML consumer consuming. |
| **Steps** | 1. Note values: `ml_consumer_messages_consumed_total`, `ml_consumer_predictions_emitted_total`.<br>2. Wait **≥30s**.<br>3. Re-query — counters **≥** previous.<br>4. Optional: Grafana Explore graph `rate(ml_consumer_predictions_emitted_total[1m])`. |
| **Expected** | Consumed and emitted increase when telemetry flows; errors remain inspectable via **`ml_consumer_errors_total`**. |
| **Precautions** | **`CONSUMER_SLOWDOWN_MS`** reduces rate — adjust expectations. |

---

## Full-stack observability walkthrough (Phases 1–5)

Run when **full stack** is up and **`validate-cluster.sh`** passes for Phases 1–5.

| Step | Action |
|------|--------|
| 1 | Open **Prometheus → Status → Targets** — confirm jobs: **`kafka-broker-1-jmx`**, **`kafka-broker-2-jmx`**, **`kafka-broker-3-jmx`**, **`schema-registry`**, **`aerostream-producer`**, **`kafka-connect`**, **`aerostream-stream-processor`**, **`aerostream-ml-consumer`** all **UP**. |
| 2 | Open **Grafana → Explore** — e.g. `up{job=~"kafka-broker-[123]-jmx|aerostream-.*|kafka-connect|schema-registry"}` — relevant series should be **1**. |
| 3 | Open **AeroStream — Producer** dashboard — simulator **on** — panels 1–7 validated per TC-PG-P2-03. |
| 4 | **Explore** — overlay producer rate vs ML **`rate(ml_consumer_predictions_emitted_total[1m])`** — qualitative correlation under steady load. |
| 5 | Document screenshot set for demo (optional): Targets page, Producer dashboard, ML counter query. |

---

## Failure playbook (Prometheus / Grafana)

| Symptom | Checks |
|---------|--------|
| Broker targets **DOWN** or **"context deadline exceeded"** / **flapping UP** | 0) **Rule:** in `prometheus.yml`, each broker job must have **`scrape_interval` > `scrape_timeout`**. If the timeout is longer than the interval, Prometheus overlaps scrapes and the JMX HTTP server can look randomly healthy.<br>1) Confirm JAR exists (`infra/scripts/download-jmx-agent.sh`), then **restart brokers** if you added it.<br>2) **Restart Prometheus** after config changes, or `curl -X POST http://localhost:9090/-/reload` when only `prometheus.yml` changed.<br>3) Catch-all JMX rule is **off by default** in `kafka-jmx.yml` to keep scrapes fast; turn it on only for debugging.<br>4) JMX agent listens **inside** the broker on **7071** (not published to the host). From another container on `aerostream-network`, `wget`/`curl` `http://kafka-1:7071/metrics` should return `# HELP` lines within a few seconds. |
| **Schema-registry** target **404** on `/metrics` | **Cause:** port **8081** is the REST API only; Confluent SR does not expose Prometheus text there. **Fix:** scrape **`schema-registry:7072/metrics`** (javaagent in Compose). |
| **Connect** DOWN | Connect on **8083**; Debezium plugin present. |
| **Stream-processor** DOWN | Service on **8091**; Spring profile **docker**. |
| **ML consumer** DOWN | **8099** `/metrics`; consumer crashed → fix logs first. |
| Grafana **no data** in Producer panels | Datasource UID **`prometheus`**; time range includes **now**; simulator started; **15s** scrape delay. |
| Grafana datasource test fails | Prometheus container name **`prometheus`** resolvable from Grafana on **`aerostream-network`**. |

---

## Automated gate cross-reference

`infra/scripts/validate-cluster.sh` includes:

- `curl -sf http://localhost:${PROMETHEUS_PORT:-9090}/-/healthy`
- `curl -sf http://localhost:${GRAFANA_PORT:-3000}/api/health | grep -q 'ok'`

Full target-level checks remain **manual/UI** (this document) unless extended with scriptable **`/api/v1/targets`** assertions.

---

## Revision history

| Change | Notes |
|--------|------|
| Initial version | Maps `prometheus.yml`, `producer-dashboard.json`, ML `main.py` counters, Phase 4 scrape gap documented. |
| Broker scrape tuning | Per-broker jobs `kafka-broker-*-jmx`; **`scrape_interval` > `scrape_timeout`** (60–66s vs 45s) + stagger to avoid overlap flapping; RMI `KAFKA_JMX_PORT` removed; catch-all JMX rule disabled; Prometheus waits for broker health. |
| Flapping `/targets` | Fixed misconfiguration where `scrape_timeout` (120s) exceeded `scrape_interval` (30–34s), causing overlapping scrapes. |
| SR 404 + kafka-3 timeouts | Schema Registry: scrape **7072** (JMX agent), not **8081**. Kafka: **60s** timeout with **90–96s** intervals. |
