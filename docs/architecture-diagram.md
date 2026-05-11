# AeroStream — Full-stack architecture (Phases 1–5)

## Which tool to use

| Goal | Tool |
|------|------|
| **Machine-readable source + CI** | **`docs/diagrams/aerostream-architecture.mmd`** — regenerate PNG/SVG with `@mermaid-js/mermaid-cli` (see comment in `.mmd` file). |
| **Pixel-perfect slides / posters** | **diagrams.net (draw.io)** or **Excalidraw** — manual export PNG/SVG. |
| **Formal C4 / enterprise models** | **Structurizr**, **PlantUML** — heavier setup; optional for this repo. |

No extra requirements needed unless you want a **specific export** (e.g. PNG for LinkedIn, dark theme, or a **C4 Level 2** decomposition); say so and we can derive it from this source.

---

## Legend — Kafka topics (from `README.md` / `create-topics.sh` / ksqlDB)

| Topic | Partitions | RF | Format / role |
|-------|------------|-----|----------------|
| `raw-telemetry` | 20 | 3 | Avro `TelemetryEvent` — Phase 2 producer |
| `enriched-telemetry` | 20 | 3 | Avro `EnrichedTelemetryEvent` — Phase 3 Kafka Streams |
| `circuit-metadata` | 3 | 3 | compact Avro — CDC from Postgres `circuits` |
| `driver-profiles` | 5 | 3 | compact Avro — CDC from Postgres `drivers` |
| `stream-aggregates` | 10 | 3 | JSON — Phase 4 ksqlDB `AGGREGATE_METRICS` sink |
| `pit-predictions` | 5 | 3 | Avro `PitPrediction` — Phase 5 ML consumer |
| `race-outcomes` | 5 | 3 | compact — provisioned; **no producer in Phases 1–5** (reserved / future) |
| `dlq-telemetry` | 3 | 3 | bytes + headers — poison pills / failures |
| Connect internals | — | — | `connect-configs`, `connect-offsets`, `connect-status` (compact) |

---

## Diagram — visual exports (open as images)

The rendered diagram maps **every shipped phase**: infra (KRaft, SR, UI, observability), producer, Postgres + Connect + Streams enrichment, ksqlDB analytics, Python ML consumer, optional dashboard BFF, and **Prometheus scrape sources** (dashed).

| Format | Path |
|--------|------|
| **SVG** (scales cleanly; best for docs/web) | [`diagrams/aerostream-architecture.svg`](./diagrams/aerostream-architecture.svg) |
| **PNG** (slides, thumbnails) | [`diagrams/aerostream-architecture.png`](./diagrams/aerostream-architecture.png) |
| **Source** (edit + regenerate) | [`diagrams/aerostream-architecture.mmd`](./diagrams/aerostream-architecture.mmd) |

Regenerate after editing the `.mmd` file:

```bash
npx @mermaid-js/mermaid-cli -c docs/diagrams/mermaid-config.json \
  -i docs/diagrams/aerostream-architecture.mmd \
  -o docs/diagrams/aerostream-architecture.svg -w 3200 -H 2800

npx @mermaid-js/mermaid-cli -c docs/diagrams/mermaid-config.json \
  -i docs/diagrams/aerostream-architecture.mmd \
  -o docs/diagrams/aerostream-architecture.png -w 3200 -H 2800 -s 2
```

### Reading order

1. **Phase 1** — Three **KRaft** brokers, **Schema Registry**, **Kafka UI**, **JMX → Prometheus → Grafana**, scripted topic provisioning and **validate-cluster**.
2. **Phase 2** — **Producer** writes **`TelemetryEvent`** to **`raw-telemetry`**; optional **`dlq-telemetry`**; simulator REST API.
3. **Phase 3** — **Postgres** reference store → **Debezium** via **Kafka Connect** → **`circuit-metadata`** / **`driver-profiles`**; **Kafka Streams** joins → **`enriched-telemetry`**.
4. **Phase 4** — **ksqlDB** reads **`enriched-telemetry`**, emits windowed KPIs to **`stream-aggregates`** (JSON).
5. **Phase 5** — **Python** consumer reads **`enriched-telemetry`**, writes **`pit-predictions`**.
6. **Dashboard BFF** — optional **read-only** HTTP API over **`pit-predictions`** and **`stream-aggregates`**.

**Observability accuracy:** `prometheus.yml` scrapes **brokers (JMX)**, **Schema Registry**, **producer**, **Kafka Connect**, **stream-processor**, and **ml-consumer** — not ksqlDB or the BFF (those are checked via **validate-cluster** / container health instead).

---

## Simpler alternative — ASCII (from README)

For quick paste into plain-text contexts:

```
Phase 2 producer (Avro) → raw-telemetry ─┬→ Phase 3 stream-processor → enriched-telemetry ─┬→ Phase 4 ksqlDB → stream-aggregates (JSON)
                                         │                                                    └→ Phase 5 ml-consumer → pit-predictions (Avro)
Phase 3: Postgres ─ CDC (Debezium/Connect) → circuit-metadata, driver-profiles ────────┘

Phase 1: KRaft Kafka ×3 · Schema Registry · Kafka UI · Prometheus · Grafana · JMX
```
