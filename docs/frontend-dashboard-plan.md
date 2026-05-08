# AeroStream — Operations Dashboard (Frontend) — Engineering Plan

**Goal:** A single, **highly polished** web application that **showcases every phase** of AeroStream (infrastructure through ML) with **light/dark themes**, **configurable service URLs** in the UI, and **clear backend contracts** so each screen is driven by real data or explicit “backend work” follow-ups.

**Assumptions (no blockers to start):**

- **SPA stack:** TypeScript, **Vite**, **React 18+**, **Tailwind CSS**, **Radix UI** or **shadcn/ui**, **`next-themes`** (or equivalent) for system-aware light/dark.
- **CORS:** Browsers cannot call all `localhost` services cross-origin without **CORS headers** on each service. **Development:** Vite `server.proxy` per service. **Production:** one **reverse proxy** (e.g. Nginx, Caddy) or a thin **BFF** (Node/FastAPI) that adds CORS and optional auth. This plan names both “**proxy path**” and “**direct URL** (if CORS fixed)”.
- **Attractive UI:** Data-dense but calm: motorsport-inspired palette, large health states, sparkline-friendly cards, no clutter. WCAG-oriented contrast in both themes.

---

## 1. Use-case → screen → backend mapping

| Use case (showcase) | Primary screen / module | Data / control source today | Gap (if any) |
|--------------------|-------------------------|----------------------------|--------------|
| **P1** Cluster & platform health | **Operations overview** | `kafka-1/2/3` (no HTTP — use **Kafka UI** deep link or **JMX via Prometheus**), Schema Registry `GET /subjects`, Prometheus `/-/healthy`, Grafana link | Optional: small **status service** aggregating `curl` checks |
| **P1** Topic inventory | **Topics** | REST: Confluent does not expose “list topics” on SR; use **Kafka UI** embed/iframe or **admin API** if added | **Optional backend:** `GET /api/v1/topics` (BFF) wrapping `kafka-topics` or Kafka AdminClient |
| **P1** Schema governance | **Schema Registry** | `GET /subjects`, `GET /subjects/{name}/versions/latest` | None for read-only |
| **P2** Producer / simulator | **Pipeline control** | `GET/POST` **`/api/simulator/status`**, **`/start`**, **`/stop`** (producer :8090) | **Runtime EPS:** today EPS is **env at container start** only; see [§5](#5-known-gaps) |
| **P2** Producer metrics | **Ingestion metrics** | `GET /actuator/health`, `GET /actuator/prometheus` (parse key series) | None |
| **P2** DLQ / testing | **Danger zone** (collapsible) | `POST /api/test/poison-pill?count=n` (non-prod) | Wire only when `docker` profile; hide in “demo safe mode” |
| **P3** Connect + CDC | **Enrichment** | Connect `GET /connectors`, `.../status`, Postgres not directly from browser (CORS) | BFF: `GET /api/v1/connect/status` proxying Connect |
| **P3** Stream processor | **Enrichment** | `GET /actuator/health`, `GET /actuator/prometheus` (stream-processor :8091) | None |
| **P3** Reference data | **Circuits & drivers** | Optional: read sample messages from `circuit-metadata` / `driver-profiles` via **BFF** + `kafka-console-consumer` pattern is **not** browser-suitable; prefer **BFF** consumer or static seed JSON for demo | **BFF** recommended for “last N compacted” |
| **P4** ksqlDB | **Analytics** | `GET /info`, `POST /ksql` with `SHOW QUERIES;`, `DESCRIBE AGGREGATE_METRICS;` (read-only statements) | Lock down: only allow **whitelisted** ksql in BFF |
| **P4** Windowed aggregates | **Live aggregates** | Topic `stream-aggregates` — same as above: need **BFF** to consume last N or ksql `SELECT` (limited) | **BFF** `GET /api/v1/aggregates/recent?limit=50` |
| **P5** ML consumer | **ML ops** | `GET /health`, `GET /ready`, `GET /metrics` (text) | Parse Prometheus text client-side or BFF |
| **P5** Pit predictions | **Pit wall** | Topic `pit-predictions` | **BFF** `GET /api/v1/predictions/recent` or Confluent REST if enabled (not in default stack) |
| **Obs** End-to-end | **Observability hub** | Links + iframes to **Grafana** / **Prometheus** / **Kafka UI** (same-origin or new tab) | Optional embed token |

---

## 2. Information architecture (routes)

Suggested routes (React Router):

| Route | Purpose |
|-------|---------|
| `/` | **Overview** — cluster OK summary, quick links, theme toggle |
| `/pipeline` | **Telemetry pipeline** — simulator start/stop, EPS display (*read-only until backend extended*), producer & processor health |
| `/kafka` | **Kafka & topics** — links to Kafka UI, optional topic list from BFF |
| `/schemas` | **Schema Registry** — subject list + latest schema viewer (JSON tree) |
| `/cdc` | **Kafka Connect** — connector cards + RUNNING state |
| `/analytics` | **ksqlDB** — `/info`, query list, optional aggregate preview |
| `/ml` | **ML consumer** — health, ready, counters from `/metrics` |
| `/pit-wall` | **Pit predictions** — recent predictions from BFF |
| `/settings` | **Configuration** — base URLs for every service (persist `localStorage`), proxy mode toggle, “demo safe mode” |

**Theme:** global toggle (light / dark / system) in shell header; persist preference.

---

## 3. Configuration on the frontend (`/settings`)

Users (and demos on different machines) must configure **without rebuilding**:

| Setting | Example | Used for |
|---------|---------|----------|
| Producer base URL | `http://localhost:8090` | Simulator + actuator |
| Stream processor URL | `http://localhost:8091` | Actuator |
| Schema Registry URL | `http://localhost:8081` | Subjects API |
| Kafka Connect URL | `http://localhost:8083` | Connectors |
| ksqlDB URL | `http://localhost:8088` | ksql HTTP |
| ML consumer URL | `http://localhost:8099` | Health + metrics |
| Prometheus URL | `http://localhost:9090` | Optional PromQL from browser (CORS often blocked — prefer BFF) |
| Kafka UI URL | `http://localhost:8080` | Open in iframe or new tab |
| Grafana URL | `http://localhost:3000` | Link only |
| **BFF base URL** (optional) | `http://localhost:8089` | Aggregated APIs when implemented |

Validate URLs on save; show red outline if invalid.

---

## 4. Backend strategy (what exists vs what to add)

### 4.1 Today: direct service calls (via proxy)

The dashboard **can** ship **MVP** calling:

- Producer: `/api/simulator/*`, `/actuator/*`
- Stream processor: `/actuator/*`
- Schema Registry: `/subjects*`
- Connect: `/connectors*`
- ksqlDB: `/info`, `/ksql` (careful)
- ML: `/health`, `/ready`, `/metrics`

**Vite example (`vite.config.ts`):**

```ts
server: {
  proxy: {
    '/svc/producer': { target: 'http://localhost:8090', changeOrigin: true, rewrite: (p) => p.replace(/^\/svc\/producer/, '') },
    '/svc/processor': { target: 'http://localhost:8091', changeOrigin: true, rewrite: ... },
    '/svc/registry': { target: 'http://localhost:8081', changeOrigin: true, rewrite: ... },
    '/svc/connect': { target: 'http://localhost:8083', changeOrigin: true, rewrite: ... },
    '/svc/ksql': { target: 'http://localhost:8088', changeOrigin: true, rewrite: ... },
    '/svc/ml': { target: 'http://localhost:8099', changeOrigin: true, rewrite: ... },
  },
}
```

Frontend uses **relative** `/svc/producer/...` so the browser sees same-origin.

### 4.2 Recommended: thin BFF (optional repo `bff/` or `dashboard/server`)

For **pit predictions**, **aggregates**, **topic offsets**, and **safe ksql**, add a small **FastAPI** or **Express** service that:

- Uses **kafka-python** / **kafka-js** Admin API + Consumer (short poll) for “recent messages”.
- Exposes **read-only** REST: `GET /api/v1/predictions/recent`, `GET /api/v1/aggregates/recent`.
- Never exposes arbitrary ksql from the browser — only server-side whitelisted statements.

This keeps the frontend **attractive and simple** while remaining secure.

---

## 5. Known gaps

1. **Simulator EPS from UI:** `SimulatorController` only exposes **start/stop/status**, not live EPS changes. **Options:** (a) document EPS via `.env` + restart producer; (b) **Issue BE-SIM-1:** add `POST /api/simulator/rate?eventsPerSecond=200` updating a mutable rate in `TelemetrySimulator`.

2. **Kafka topic contents in browser:** no native browser Kafka consumer — **BFF** or iframe **Kafka UI** for message browsing.

3. **Prometheus from browser:** often blocked by CORS — use Grafana links or BFF PromQL proxy.

---

## 6. GitHub issues + isolated Cursor prompts

Copy each block into a GitHub issue. Run prompts **one issue at a time** in a clean branch.

---

### Issue **FE-0** — Repo scaffold & design system

**Title:** `[FE-0] Dashboard monorepo scaffold + light/dark theme`

**User story:** As a demo presenter, I want a dedicated `dashboard/` app with professional layout and themes so all later features plug in consistently.

**Scope:**

- `dashboard/` with Vite + React + TS + ESLint + Prettier.
- Tailwind + CSS variables for **light** and **dark** (e.g. `--background`, `--card`, `--accent` racing stripe accent).
- **shadcn/ui** or Radix primitives: Button, Card, Tabs, Switch, Sheet, Tooltip.
- **`next-themes`** (or `ThemeProvider`) with toggle in header + `localStorage`.
- Shell layout: sidebar nav (links from §2), main content, footer with stack version from `package.json`.

**Acceptance criteria:**

- `pnpm dev` / `npm run dev` starts on port **5173** (or documented).
- Theme persists across refresh; system preference respected on first visit.

**Isolated Cursor prompt:**

> Create `dashboard/` at repo root: Vite + React + TypeScript. Add Tailwind, `next-themes`, and a small set of shadcn/ui components. Implement a two-column shell: collapsible sidebar with nav links (Overview, Pipeline, Kafka, Schemas, CDC, Analytics, ML, Pit Wall, Settings), top bar with theme toggle (sun/moon) and title “AeroStream Control”. Use CSS variables for light/dark backgrounds (#fafafa / #0c0e12) and a single accent (e.g. crimson #e10600). No data fetching yet — placeholder cards only. Add README with `npm install` and `npm run dev`.

---

### Issue **FE-1** — Settings page & runtime config

**Title:** `[FE-1] Service URL configuration persistence`

**Scope:** `/settings` page with inputs for all URLs in §3; **Zod** validation; save to `localStorage` key `aerostream.settings.v1`; React Context `useServices()` returning URLs.

**Acceptance:** Reload preserves values; invalid URL shows inline error.

**Isolated Cursor prompt:**

> In `dashboard/`, add React Router and a Settings page at `/settings`. Store service base URLs in localStorage with Zod schema. Provide `ServicesProvider` context. Default URLs match README localhost ports. No API calls yet.

---

### Issue **FE-2** — API client layer + TanStack Query

**Title:** `[FE-2] Typed HTTP client and React Query`

**Scope:** `fetch` wrappers with typed errors; TanStack Query for caching; all requests use **relative** `/svc/...` paths expecting Vite proxy (document in README).

**Acceptance:** Health checks use `useQuery` with 10s refetch interval (toggleable).

**Isolated Cursor prompt:**

> Add `@tanstack/react-query`, create `lib/api/client.ts` with `getJson<T>(path)`. Document in `dashboard/README.md` the required `vite.config.ts` proxy table matching AeroStream ports. Add a `HealthStrip` component that calls `/svc/producer/actuator/health` and `/svc/processor/actuator/health` and shows UP/DOWN badges.

---

### Issue **FE-3** — Overview dashboard

**Title:** `[FE-3] Operations overview page`

**Scope:** Cards for: Schema Registry subjects count (from `GET /subjects`), Connect cluster reachable, ksql `/info`, ML `/ready`. External links: Kafka UI, Grafana, Prometheus.

**Acceptance:** Single glance shows green/yellow/red per subsystem.

**Isolated Cursor prompt:**

> Implement route `/` with cards wired to Schema Registry subjects list, Connect root, ksqlDB `/info`, ML `/health` and `/ready`. Use TanStack Query. Graceful loading skeletons and error toasts. Use design tokens only.

---

### Issue **FE-4** — Pipeline control (Phase 2)

**Title:** `[FE-4] Simulator control panel`

**Scope:** Buttons Start / Stop; poll `GET /api/simulator/status`; show running, total published if present in JSON.

**Acceptance:** Start/stop works against live producer; rate shows from status if available.

**Isolated Cursor prompt:**

> Add `/pipeline` with SimulatorControl: POST start/stop to `/svc/producer/api/simulator/start|stop`, GET status every 2s when tab focused. Display JSON in a pretty collapsible panel. Add warning banner: “Events per second is set when the producer container starts (SIMULATOR_EVENTS_PER_SECOND).”

---

### Issue **FE-5** — Schema Registry browser

**Title:** `[FE-5] Subjects and schema viewer`

**Scope:** List subjects; click → latest schema JSON (pretty tree).

**Acceptance:** Can open `EnrichedTelemetryEvent` subject when registered.

**Isolated Cursor prompt:**

> Implement `/schemas`: fetch `/svc/registry/subjects`, then for selected subject fetch latest version schema from Registry REST. Use `@monaco-editor/react` or simple `<pre>` with syntax highlight. Sidebar subject search.

---

### Issue **FE-6** — Kafka Connect dashboard

**Title:** `[FE-6] CDC connector status`

**Scope:** `GET /connectors` then each `.../status`; table with connector / task RUNNING.

**Acceptance:** Matches `validate-cluster.sh` expectations visually.

**Isolated Cursor prompt:**

> Add `/cdc`: list connectors from Connect API, show status cards with color-coded states. Link to Connect REST docs in footer.

---

### Issue **FE-7** — ksqlDB analytics view

**Title:** `[FE-7] ksqlDB info and queries (read-only)`

**Scope:** Show `/info`; POST `SHOW QUERIES;` via ksql — **only from frontend if proxy allows**; otherwise stub + “Run from CLI” until BFF.

**Acceptance:** Query IDs visible when ksql allows.

**Isolated Cursor prompt:**

> Implement `/analytics`: fetch ksql `/info`. Add button “Refresh queries” that POSTs ksql `SHOW QUERIES;` to `/svc/ksql/ksql` with JSON body per Confluent docs. Parse response safely; if CORS/preflight fails, show instructions to use BFF (FE-9).

---

### Issue **FE-8** — ML consumer dashboard

**Title:** `[FE-8] ML health and counters`

**Scope:** `/health`, `/ready`, parse `/metrics` (Prometheus text) for `ml_consumer_*` counters.

**Acceptance:** Counters update every 5s.

**Isolated Cursor prompt:**

> Add `/ml`: display health, ready (503 handling), and parse Prometheus text from `/svc/ml/metrics` for the three ml_consumer counters. Chart last N samples in memory (sparkline).

---

### Issue **FE-9** — BFF service (optional but recommended)

**Title:** `[FE-9] Read-only BFF for Kafka topic previews`

**Scope:** Small FastAPI app in `bff/` with **env** `KAFKA_BOOTSTRAP`, `SR_URL`. Endpoints: `GET /api/v1/pit-predictions/recent?limit=20` (Avro decode via fastavro + SR), `GET /api/v1/stream-aggregates/recent?limit=20` (JSON).

**Acceptance:** Dashboard Pit Wall and Analytics consume BFF; docker-compose service `dashboard-bff`.

**Isolated Cursor prompt:**

> Add `bff/` FastAPI project with endpoints consuming from `pit-predictions` and `stream-aggregates` using confluent-kafka-python, AvroDeserializer, limit 20, timeout 5s. Dockerfile + compose service on 8089. CORS `*` for dev. Unit-test with mock.

---

### Issue **FE-10** — Pit Wall UI

**Title:** `[FE-10] Pit predictions wall`

**Scope:** Table: car_id, timestamp, probability, recommend_pit, model_version — fed from **FE-9** or placeholder mock.

**Acceptance:** Live rows when BFF + stack running.

**Isolated Cursor prompt:**

> Add `/pit-wall` with data grid (TanStack Table): columns from PitPrediction. Wire to `bff` `/api/v1/pit-predictions/recent` with fallback mock array when BFF offline. Highlight `recommend_pit` rows.

---

### Issue **FE-11** — Polish & a11y pass

**Title:** `[FE-11] Accessibility, motion, and empty states`

**Scope:** Focus rings, aria labels, reduced-motion respect, empty/error illustrations.

**Acceptance:** Lighthouse a11y score ≥ 90 on main routes.

**Isolated Cursor prompt:**

> Audit all dashboard routes: add empty states (“Start the stack with docker compose”), loading skeletons, error boundaries. Respect `prefers-reduced-motion`. Ensure contrast in dark mode passes WCAG AA for body text.

---

### Issue **BE-SIM-1** (backend, optional)

**Title:** `[BE-SIM-1] Runtime simulator rate API**

**Scope:** Extend `TelemetrySimulator` + `SimulatorController` with `POST /api/simulator/rate?eventsPerSecond=` adjusting scheduled rate without container restart.

**Acceptance:** FE-4 can slider-bind to this endpoint.

---

## 7. Execution order (recommended)

1. **FE-0 → FE-1 → FE-2** (foundation)  
2. **FE-3, FE-4, FE-5, FE-6, FE-7, FE-8** (feature slices in parallel after FE-2)  
3. **FE-9** when topic previews are required  
4. **FE-10** after FE-9 (or with mocks)  
5. **FE-11** before public demo  
6. **BE-SIM-1** when EPS slider is mandatory  

---

## 8. Risk register

| Risk | Mitigation |
|------|------------|
| CORS | Vite proxy in dev; Nginx/BFF in prod |
| ksql injection | Never send arbitrary SQL from browser; whitelist in BFF |
| Secrets | Dashboard never stores Kafka credentials — BFF uses env |
| Performance | Poll intervals configurable; pause when tab hidden (`document.visibilityState`) |

---

## 9. Related docs

- [`dashboard-testing-fe3-fe8.md`](./dashboard-testing-fe3-fe8.md) — **manual dashboard QA** (backend steps, UI expectations, FE-3–FE-8 acceptance)  
- [`demo-system-test-suite.md`](./demo-system-test-suite.md) — manual validation  
- Phase guides `phase-1` … `phase-5` — domain semantics  

---

**Document status:** Planning artifact — implement issues independently; adjust ports if `.env` differs.
