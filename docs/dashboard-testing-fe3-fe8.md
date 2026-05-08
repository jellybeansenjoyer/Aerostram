# Dashboard manual testing — FE-3 through FE-8

This guide ties **backend preparation**, **dashboard checks** (Vite dev server on port **5173**), and **acceptance criteria** from [`frontend-dashboard-plan.md`](frontend-dashboard-plan.md) for features **FE-3 → FE-8**.  
Assumes FE-0–FE-2 are already implemented (shell, Settings, `/svc/...` proxy, HealthStrip).

---

## 1. Prerequisites

| Requirement | Notes |
|-------------|--------|
| Docker | Stack runs via `docker compose`. |
| Repo `.env` | `cp .env.example .env` (adjust ports if needed). |
| Dashboard | From repo root: `cd dashboard && npm install && npm run dev` → **http://localhost:5173**. |
| Proxy | All API calls use same-origin **`/svc/...`** paths; Vite proxies to defaults in `dashboard/vite.config.ts` (override with `VITE_PROXY_*` if needed). |

**Optional sanity** before UI testing:

```bash
# Producer & processor actuator (HealthStrip / FE-2 baseline)
curl -s http://127.0.0.1:8090/actuator/health
curl -s http://127.0.0.1:8091/actuator/health
```

---

## 2. Recommended full backend bootstrap (one sequence)

Run from **repository root** so scripts resolve paths correctly.

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `bash infra/scripts/init-kafka-storage.sh` | KRaft cluster id (**first-time / clean volumes** only). |
| 2 | `docker compose up -d` | Start Kafka, Schema Registry, producer, Connect, Postgres, stream-processor, ksqlDB, ML consumer, observability, etc. |
| 3 | `bash infra/scripts/create-topics.sh` | Create AeroStream topics. |
| 4 | `bash infra/scripts/configure-schema-registry.sh` | Registry compatibility. |
| 5 | Run producer + stream-processor briefly (or leave stack up) so **enriched-telemetry** value schema appears in Schema Registry. |
| 6 | `bash infra/scripts/deploy-ksql-queries.sh` | Phase 4 ksqlDB persistent queries (needs step 5). |
| 7 | `bash infra/scripts/validate-cluster.sh` | PASS/FAIL gate across phases (optional but recommended). |

**Minimal subsets:** Some routes work with fewer services (called out per feature below). For a **single full pass** of FE-3–FE-8, use the full sequence above.

---

## 3. FE-3 — Operations overview (`/`)

### Backend / services

| Dependency | Default URL | Role |
|------------|----------------|------|
| Producer | `:8090` | HealthStrip producer actuator |
| Stream processor | `:8091` | HealthStrip processor actuator |
| Schema Registry | `:8081` | Subject **count** card |
| Kafka Connect | `:8083` | Connect **connectors** card |
| ksqlDB | `:8088` | **`/info`** card |
| ML consumer | `:8099` | **Health + ready** combined card |
| Settings URLs | — | **External tools** buttons (Kafka UI, Grafana, Prometheus) |

**Optional curl:**

```bash
curl -s http://127.0.0.1:8081/subjects | head -c 200
curl -s http://127.0.0.1:8083/connectors
curl -s http://127.0.0.1:8088/info
curl -s http://127.0.0.1:8099/health
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8099/ready
```

### Frontend — what to verify

1. Open **`/`**. **HealthStrip**: producer and stream processor show **UP** when services are healthy (tooltips on hover).
2. **Schema Registry** card: non-error state; **yellow** if zero subjects, **green** if at least one subject.
3. **Kafka Connect** card: **green** when `GET /connectors` succeeds; shows connector count.
4. **ksqlDB** card: version (or similar) from **`/info`**; **yellow** if version missing in parsed payload.
5. **ML consumer** card: health **healthy**; ready **yellow** if `/ready` returns 503 until Kafka assignment.
6. **External tools**: buttons open **Settings** URLs in a new tab (configure under **Settings** if links are wrong).
7. Stop one backend (e.g. Registry), wait for refetch (~30s) or refresh: card goes **red**, **Sonner** error toast may appear.

### Acceptance (plan §FE-3)

| Criterion | How you know it’s met |
|-----------|------------------------|
| Single glance **green / yellow / red** per subsystem | Colored left border + dot on each overview card. |
| External links (Kafka UI, Grafana, Prometheus) | Three buttons use persisted Settings URLs. |
| Loading skeletons + error toasts | Skeletons while loading; toasts + inline errors on failure. |

---

## 4. FE-4 — Pipeline simulator (`/pipeline`)

### Backend / services

| Dependency | Notes |
|------------|--------|
| **Producer** `:8090` | Required for `/api/simulator/*`. |

**Optional curl:**

```bash
curl -s -X POST http://127.0.0.1:8090/api/simulator/start
curl -s http://127.0.0.1:8090/api/simulator/status
```

### Frontend — what to verify

1. Open **`/pipeline`**. Read **amber banner** about **`SIMULATOR_EVENTS_PER_SECOND`** at container start.
2. **Start** → success toast; **Stop** → success toast.
3. **Metrics** update: Running, Events/sec (from JSON), Total published, Active cars.
4. **Raw status JSON** expandable panel matches API output.
5. With the **browser tab in background**, polling should **pause**; focus the tab again → updates resume ~every **2s**.

### Acceptance (plan §FE-4)

| Criterion | Met when |
|-----------|----------|
| Start/stop against live producer | Buttons succeed when producer is up. |
| Rate from status | **Events / sec (configured)** shows value from status JSON when present. |
| Poll every 2s when tab focused | Visible only with tab focused (`visibilityState`). |
| EPS warning banner | Amber banner text matches plan intent. |

---

## 5. FE-5 — Schema Registry browser (`/schemas`)

### Backend / services

| Dependency | Notes |
|------------|--------|
| **Schema Registry** `:8081` | Required. |
| **Schemas registered** | Producer / pipeline registers subjects (e.g. `raw-telemetry-value`, later **EnrichedTelemetryEvent**). |

**Optional curl:**

```bash
curl -s http://127.0.0.1:8081/subjects
curl -s "http://127.0.0.1:8081/subjects/EnrichedTelemetryEvent/versions/latest" | head -c 400
```

### Frontend — what to verify

1. Open **`/schemas`**. Left list matches **`GET /subjects`** (refetch ~45s).
2. **Search** filters the list.
3. Click a subject → right panel shows **version**, **schema id**, **type**, **schema body** as **JsonTree** (or `<pre>` if not JSON).
4. Deep link: **`/schemas?subject=EnrichedTelemetryEvent`** selects subject when present in the catalog.

### Acceptance (plan §FE-5)

| Criterion | Met when |
|-----------|----------|
| Open **EnrichedTelemetryEvent** when registered | Subject appears in list and latest schema loads without error. |

---

## 6. FE-6 — CDC & Kafka Connect (`/cdc`)

### Backend / services

| Dependency | Notes |
|------------|--------|
| **Kafka Connect** `:8083` | Required. |
| **Connectors deployed** | e.g. Phase 3 Debezium: `aerostream-circuits-connector`, `aerostream-drivers-connector` (see `validate-cluster.sh`). |

**Optional curl:**

```bash
curl -s http://127.0.0.1:8083/connectors
curl -s http://127.0.0.1:8083/connectors/aerostream-circuits-connector/status
```

### Frontend — what to verify

1. Open **`/cdc`**. Each connector shows **connector state** and **task** rows.
2. When connector and **all** tasks are **RUNNING**, card has **green** left border; otherwise **amber**.
3. Footer links **Confluent Connect REST** documentation.
4. Data refreshes ~every **20s**.

### Acceptance (plan §FE-6)

| Criterion | Met when |
|-----------|----------|
| List + per-connector **status** | Matches Connect REST; aligns visually with **RUNNING** expectations in `validate-cluster.sh`. |
| Color-coded states | Green vs amber (and task-level badges) reflect RUNNING vs not. |
| Footer doc link | Present. |

---

## 7. FE-7 — ksqlDB analytics (`/analytics`)

### Backend / services

| Dependency | Notes |
|------------|--------|
| **ksqlDB** `:8088` | Required for `/info` and `POST /ksql`. |
| **Queries deployed** | `bash infra/scripts/deploy-ksql-queries.sh` after enriched schema exists. |

**Optional curl:**

```bash
curl -s http://127.0.0.1:8088/info
curl -s -X POST http://127.0.0.1:8088/ksql \
  -H "Content-Type: application/json" \
  -d '{"ksql":"SHOW QUERIES;","streamsProperties":{"ksql.streams.auto.offset.reset":"earliest"}}' | head -c 500
```

### Frontend — what to verify

1. Open **`/analytics`**. **Server info** shows ksql version / service id from **`/info`**.
2. **Refresh queries** runs **`SHOW QUERIES`**; table lists **query IDs** (and query text) when parsing succeeds.
3. Initial load also triggers **`SHOW QUERIES`** (cached ~30s stale time).
4. If POST fails: **amber** panel with CLI / **FE-9** BFF guidance and optional error body.

### Acceptance (plan §FE-7)

| Criterion | Met when |
|-----------|----------|
| **`/info`** visible | Card populated when ksql is up. |
| **Query IDs** when ksql allows | Rows appear after successful `SHOW QUERIES` parse. |
| Failure path | User sees fallback instructions (proxy/BFF), not a silent failure. |

---

## 8. FE-8 — ML consumer (`/ml`)

### Backend / services

| Dependency | Notes |
|------------|--------|
| **ML consumer** `:8099` | FastAPI app: `/health`, `/ready`, `/metrics`. |

**Optional curl:**

```bash
curl -s http://127.0.0.1:8099/health
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8099/ready
curl -s http://127.0.0.1:8099/metrics | grep ml_consumer_
```

### Frontend — what to verify

1. Open **`/ml`**. **Health** and **Ready** cards match API (ready may be **not_ready** / 503 until Kafka assigned).
2. Three counters: **messages consumed**, **predictions emitted**, **errors** — values from Prometheus text.
3. **Sparklines** accumulate samples; **Reset sparklines** clears history.
4. Metrics poll every **5s** (aligned with refetch interval).

### Acceptance (plan §FE-8)

| Criterion | Met when |
|-----------|----------|
| Health + ready (503 OK) | Display matches `/health` and `/ready` behavior. |
| **`ml_consumer_*` counters** | All three appear when metrics endpoint exposes them. |
| **Sparkline / last N in memory** | Chart updates over time; reset works. |
| **5s refresh** | Values/sparklines update on that cadence while page is open. |

---

## 9. Quick PASS/FAIL checklist

Run backend (full recommended stack), then dashboard `npm run dev`, and tick mentally:

| ID | Route | PASS if |
|----|--------|---------|
| FE-3 | `/` | Health strip UP; 4 subsystem cards + links behave as §3; colors/toasts on failure. |
| FE-4 | `/pipeline` | Start/stop works; status + JSON panel; tab-focused polling; EPS banner. |
| FE-5 | `/schemas` | Subjects list, search, latest schema tree; deep link works when subject exists. |
| FE-6 | `/cdc` | Connectors listed; tasks RUNNING reflected; doc footer. |
| FE-7 | `/analytics` | `/info` OK; `SHOW QUERIES` returns IDs when deployed; error panel if POST fails. |
| FE-8 | `/ml` | Health/ready + 3 counters + sparklines; ~5s updates. |

---

## 10. Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| All `/svc/...` fail | Services down or wrong **proxy** target — check `docker compose ps` and `dashboard/vite.config.ts` / env. |
| FE-7 POST fails from browser | ksql not reachable or proxy rejects POST — use curl against `:8088` from host; use CLI or future BFF (FE-9). |
| FE-5 empty subjects | Registry up but no schemas — run producer / pipeline to register. |
| FE-6 empty connectors | Connect up but no connectors — deploy Phase 3 connectors. |

---

*Last aligned with dashboard features FE-3–FE-8 and repo README Quick Start.*
