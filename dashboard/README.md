# AeroStream dashboard

Vite + React + TypeScript control plane UI for the AeroStack local stack.

## Scripts

| Command | Description |
|--------|-------------|
| `npm install` | Install dependencies |
| `npm run dev` | Dev server on **http://localhost:5173** (strict port) |
| `npm run build` | Typecheck + production bundle to `dist/` |
| `npm run preview` | Preview production build on port 5173 |
| `npm run lint` | ESLint |
| `npm run format` | Prettier write |

## Proxy (`vite.config.ts`)

The browser uses same-origin **`/svc/...`** paths; Vite rewrites to backend ports (defaults match the repo README).

| Path prefix | Default target | Env override |
|-------------|----------------|--------------|
| `/svc/producer` | `http://127.0.0.1:8090` | `VITE_PROXY_PRODUCER` |
| `/svc/processor` | `http://127.0.0.1:8091` | `VITE_PROXY_PROCESSOR` |
| `/svc/registry` | `http://127.0.0.1:8081` | `VITE_PROXY_REGISTRY` |
| `/svc/connect` | `http://127.0.0.1:8083` | `VITE_PROXY_CONNECT` |
| `/svc/ksql` | `http://127.0.0.1:8088` | `VITE_PROXY_KSQL` |
| `/svc/ml` | `http://127.0.0.1:8099` | `VITE_PROXY_ML` |

Health checks on **Overview** call:

- `GET /svc/producer/actuator/health`
- `GET /svc/processor/actuator/health`

Start the producer and stream-processor containers (or processes) so these return **UP**.

## Settings

**Settings** persists service base URLs under `localStorage` key `aerostream.settings.v1` (validated with Zod). URLs are used by future routes; actuator checks use the proxy paths above regardless.

## Theme

Light / dark / system is stored via `next-themes` (`localStorage` key `aerostream.theme`).

## Operations overview (FE-3)

Route `/` — besides the **HealthStrip**, subsystem cards call (via proxy):

| Card | API |
|------|-----|
| Schema Registry | `GET /svc/registry/subjects` (count → green / yellow if empty / red on error) |
| Kafka Connect | `GET /svc/connect/connectors` (reachable = green) |
| ksqlDB | `GET /svc/ksql/info` |
| ML consumer | `GET /svc/ml/health` and `GET /svc/ml/ready` (503 until Kafka ready → yellow) |
| External tools | Links use URLs from **Settings** (Kafka UI, Grafana, Prometheus) |

Failed requests also raise a **Sonner** toast. Cards refetch about every 30s.

## Pipeline simulator (FE-4)

Route `/pipeline` — **Start** / **Stop** call `POST /svc/producer/api/simulator/start|stop`. Status uses `GET /svc/producer/api/simulator/status` every **2 seconds** only while the **browser tab is visible** (`document.visibilityState`). The yellow banner documents that EPS comes from `SIMULATOR_EVENTS_PER_SECOND` at container start.

## Schema Registry browser (FE-5)

Route `/schemas` — loads **`GET /svc/registry/subjects`**, sidebar **search** filter, click a subject to fetch **`GET /svc/registry/subjects/{subject}/versions/latest`**. Parsed Avro/JSON schema strings render as an expandable **tree** (`JsonTree`); invalid JSON falls back to a monospace `<pre>`. Selection syncs to **`?subject=`** for deep links (e.g. `EnrichedTelemetryEvent`). Subject list refetches about every **45s**.

## CDC & Kafka Connect (FE-6)

Route `/cdc` — **`GET /svc/connect/connectors`** then each **`/svc/connect/connectors/{name}/status`**. Cards show connector state and per-task states (**RUNNING** highlighted vs failures). Refetches about every **20s**. Footer links Confluent **Kafka Connect REST API** docs.

## ksqlDB analytics (FE-7)

Route `/analytics` — **`GET /svc/ksql/info`** and **`POST /svc/ksql/ksql`** with JSON body (same shape as `infra/scripts/deploy-ksql-queries.sh`) to run **`SHOW QUERIES;`**. Table lists persistent **query IDs** when the response parses. **Refresh queries** refetches; failures show an amber panel with CLI/BFF (FE-9) guidance.

## ML consumer (FE-8)

Route `/ml` — **`GET /svc/ml/health`**, **`GET /svc/ml/ready`** (503 OK until ready), **`GET /svc/ml/metrics`** Prometheus text. Parses **`ml_consumer_*`** counters and shows values plus **in-memory sparklines** (last **72** samples). Metrics poll every **5s**.
