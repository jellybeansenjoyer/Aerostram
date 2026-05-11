# AeroStream — Full-stack architecture (Phases 1–5)

## Which tool to use

| Goal | Tool |
|------|------|
| **Machine-readable source + CI** | **`docs/diagrams/aerostream-architecture.mmd`** — run `bash infra/scripts/render-architecture-diagrams.sh` after edits. |
| **Pixel-perfect slides / posters** | **diagrams.net (draw.io)** or **Excalidraw** — manual export PNG/SVG. |
| **Formal C4 / enterprise models** | **Structurizr**, **PlantUML** — heavier setup; optional for this repo. |

**Exports:** light + dark SVG/PNG, and LinkedIn-sized dark PNGs (`fit-linkedin.py`), are described below.

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

### Light theme (default)

| Format | Path |
|--------|------|
| **SVG** | [`diagrams/aerostream-architecture.svg`](./diagrams/aerostream-architecture.svg) |
| **PNG** | [`diagrams/aerostream-architecture.png`](./diagrams/aerostream-architecture.png) |

### Dark theme

| Format | Path |
|--------|------|
| **SVG** | [`diagrams/aerostream-architecture-dark.svg`](./diagrams/aerostream-architecture-dark.svg) |
| **PNG** | [`diagrams/aerostream-architecture-dark.png`](./diagrams/aerostream-architecture-dark.png) |

Config: [`diagrams/mermaid-config-dark.json`](./diagrams/mermaid-config-dark.json) (`"theme": "dark"`).

### LinkedIn — article / feed image (dark, letterboxed)

LinkedIn recommends **1200 × 627** px (≈1.91∶1) for link previews and feed images. The diagram is scaled **down to fit** inside that frame with padding (RGB **30,30,30**) so nothing is cropped.

| Use | Dimensions | Path |
|-----|------------|------|
| **Standard** | **1200 × 627** px | [`diagrams/aerostream-architecture-linkedin-1200x627-dark.png`](./diagrams/aerostream-architecture-linkedin-1200x627-dark.png) |
| **Retina / sharper upload** | **2400 × 1254** px (2×) | [`diagrams/aerostream-architecture-linkedin-2400x1254-dark@2x.png`](./diagrams/aerostream-architecture-linkedin-2400x1254-dark@2x.png) |

Letterboxing script: [`diagrams/fit-linkedin.py`](./diagrams/fit-linkedin.py) (requires **Pillow**).

### Source

| | Path |
|--|------|
| **Mermaid source** | [`diagrams/aerostream-architecture.mmd`](./diagrams/aerostream-architecture.mmd) |

### Regenerate everything

One shot (Mermaid CLI + light/dark PNG/SVG + LinkedIn fits; installs Pillow into `docs/diagrams/.pillow_vendor/`, gitignored):

```bash
bash infra/scripts/render-architecture-diagrams.sh
```

Manual steps — light PNG/SVG:

```bash
npx @mermaid-js/mermaid-cli -c docs/diagrams/mermaid-config.json \
  -i docs/diagrams/aerostream-architecture.mmd \
  -o docs/diagrams/aerostream-architecture.svg -w 3200 -H 2800

npx @mermaid-js/mermaid-cli -c docs/diagrams/mermaid-config.json \
  -i docs/diagrams/aerostream-architecture.mmd \
  -o docs/diagrams/aerostream-architecture.png -w 3200 -H 2800 -s 2
```

Manual — dark PNG/SVG: use `mermaid-config-dark.json` and outputs `aerostream-architecture-dark.svg` / `.png`. Then run `python3 docs/diagrams/fit-linkedin.py` with Pillow installed (or run `render-architecture-diagrams.sh`).

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
